import argparse
import asyncio
import logging
import os
import random
import resource
import socket
import sys
import traceback
from datetime import datetime
from multiprocessing import Process
from os.path import exists, join, isabs

import qcg.pilotjob.profile
from qcg.pilotjob.config import Config
from qcg.pilotjob.errors import InvalidArgument
from qcg.pilotjob.fileinterface import FileInterface
from qcg.pilotjob.manager import DirectManager
from qcg.pilotjob.partitions import GovernorManager
from qcg.pilotjob.receiver import Receiver
from qcg.pilotjob.reports import get_reporter
from qcg.pilotjob.zmqinterface import ZMQInterface


class QCGPMService:
    """QCG Pilot Job manager instance.

    Attributes:
        exitCode (int): the result exit code
        _args: parsed arguments by argparse
        _conf (dict(str,str)): configuration created based on arguments
        _wd (path): path to the working directory
        _aux_dir (path): path to the auxiliary directory where all logs and temporary QCG-PilotJob files will be stored
        _logHandler (logging.FileHandler): file logging handler
        _ifaces (list(Interface)): list of active input interfaces
        _job_reporter (JobReport): job reporter instance
        _receiver (Receiver): the receiver instance
        _manager (Manager): the manager instance
    """

    def _parse_args(self, args):
        """Return parsed for command line arguments.

        Validate arguments and initialize _args attribute

        Args:
            args (str[]) - command line arguments, if None the command line arguments are parsed
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--net',
                            help='enable network interface',
                            action='store_true')
        parser.add_argument('--net-port',
                            help='port to listen for network interface (implies --net)',
                            type=int, default=None)
        parser.add_argument('--net-port-min',
                            help='minimum port range to listen for network interface if exact port number is not '
                                 'defined (implies --net)',
                            type=int, default=None)
        parser.add_argument('--net-port-max',
                            help='maximum port range to listen for network interface if exact port number is not '
                                 'defined (implies --net)',
                            type=int, default=None)
        parser.add_argument('--file',
                            help='enable file interface',
                            action='store_true')
        parser.add_argument('--file-path',
                            help='path to the request file (implies --file)',
                            default=None)
        parser.add_argument('--wd',
                            help='working directory for the service',
                            default=Config.EXECUTOR_WD.value['default'])
        parser.add_argument('--envschema',
                            help='job environment schema [auto|slurm]',
                            default='auto')
        parser.add_argument('--resources',
                            help='source of information about available resources [auto|slurm|local] as well as a '
                                 'method of job execution (through local processes or as a Slurm sub jobs)',
                            default=Config.RESOURCES.value['default'])
        parser.add_argument('--report-format',
                            help='format of job report file [text|json]',
                            default=Config.REPORT_FORMAT.value['default'])
        parser.add_argument('--report-file',
                            help='name of the job report file',
                            default=Config.REPORT_FILE.value['default'])
        parser.add_argument('--nodes',
                            help='configuration of available resources (implies --resources local)',
                            )
        parser.add_argument('--log',
                            help='log level',
                            choices=['critical', 'error', 'warning', 'info', 'debug', 'notset'],
                            default=Config.LOG_LEVEL.value['default'])
        parser.add_argument('--system-core',
                            help='reserve one of the core for the QCG-PJM',
                            default=False, action='store_true')
        parser.add_argument('--disable-nl',
                            help='disable custom launching method',
                            default=Config.DISABLE_NL.value['default'], action='store_true')
        parser.add_argument('--show-progress',
                            help='print information about executing tasks',
                            default=Config.PROGRESS.value['default'], action='store_true')
        parser.add_argument('--governor',
                            help='run manager in the governor mode, where jobs will be scheduled to execute to the '
                                 'dependant managers',
                            default=Config.GOVERNOR.value['default'], action='store_true')
        parser.add_argument('--parent',
                            help='address of the parent manager, current instance will receive jobs from the parent '
                                 'manaqger',
                            default=Config.PARENT_MANAGER.value['default'])
        parser.add_argument('--id',
                            help='optional manager instance identifier - will be generated automatically when not '
                                 'defined',
                            default=Config.MANAGER_ID.value['default'])
        parser.add_argument('--tags',
                            help='optional manager instance tags separated by commas',
                            default=Config.MANAGER_TAGS.value['default'])
        parser.add_argument('--slurm-partition-nodes',
                            help='split Slurm allocation by given number of nodes, where each group will be '
                                 'controlled by separate manager (implies --governor)',
                            type=int, default=None)
        parser.add_argument('--slurm-limit-nodes-range-begin',
                            help='limit Slurm allocation to specified range of nodes (starting node)',
                            type=int, default=None)
        parser.add_argument('--slurm-limit-nodes-range-end',
                            help='limit Slurm allocation to specified range of nodes (ending node)',
                            type=int, default=None)

        self._args = parser.parse_args(args)

        if self._args.slurm_partition_nodes:
            # imply '--governor'
            self._args.governor = True

        if self._args.governor or self._args.parent:
            # imply '--net' in case of hierarchy scheduling - required for inter-manager communication
            self._args.net = True

        if self._args.net_port or self._args.net_port_min or self._args.net_port_max:
            # imply '--net' if port or one of the range has been defined
            self._args.net = True

            if not self._args.net_port_min:
                self._args.net_port_min = int(Config.ZMQ_PORT_MIN_RANGE.value['default'])

            if not self._args.net_port_max:
                self._args.net_port_max = int(Config.ZMQ_PORT_MAX_RANGE.value['default'])

        if self._args.net:
            # set default values for port min & max if '--net' has been defined
            if not self._args.net_port_min:
                self._args.net_port_min = int(Config.ZMQ_PORT_MIN_RANGE.value['default'])

            if not self._args.net_port_max:
                self._args.net_port_max = int(Config.ZMQ_PORT_MAX_RANGE.value['default'])

        if self._args.file and not self._args.file_path:
            # set default file path if interface has been enabled but path not defined
            self._args.file_path = Config.FILE_PATH.value['default']

        if self._args.file_path:
            # enable file interface if path has been defined
            self._args.file = True

    def _create_config(self):
        """Based on arguments create QCG-PilotJob configuration.

        Initialized ``_conf`` attribute.
        """
        manager_id = self._args.id
        if not manager_id:
            manager_id = '{}.{}'.format(socket.gethostname(), random.randrange(10000))

        manager_tags = [manager_id]
        if self._args.tags:
            manager_tags.extend(self._args.tags.split(','))

        self._conf = {
            Config.EXECUTOR_WD: self._args.wd,
            Config.EXECUTION_NODES: self._args.nodes,
            Config.ENVIRONMENT_SCHEMA: self._args.envschema,
            Config.FILE_PATH: self._args.file_path,
            Config.ZMQ_PORT: self._args.net_port,
            Config.ZMQ_PORT_MIN_RANGE: self._args.net_port_min,
            Config.ZMQ_PORT_MAX_RANGE: self._args.net_port_max,
            Config.REPORT_FORMAT: self._args.report_format,
            Config.REPORT_FILE: self._args.report_file,
            Config.LOG_LEVEL: self._args.log,
            Config.SYSTEM_CORE: self._args.system_core,
            Config.DISABLE_NL: self._args.disable_nl,
            Config.PROGRESS: self._args.show_progress,
            Config.GOVERNOR: self._args.governor,
            Config.PARENT_MANAGER: self._args.parent,
            Config.MANAGER_ID: manager_id,
            Config.MANAGER_TAGS: manager_tags,
            Config.SLURM_PARTITION_NODES: self._args.slurm_partition_nodes,
            Config.SLURM_LIMIT_NODES_RANGE_BEGIN: self._args.slurm_limit_nodes_range_begin,
            Config.SLURM_LIMIT_NODES_RANGE_END: self._args.slurm_limit_nodes_range_end,
        }

    def __init__(self, args=None):
        """Initialize QCG Pilot Job manager instance.

        Parse arguments, create configuration, create working & auxiliary directories, setup logging and interfaces.

        Args:
            args (str[]) - command line arguments, if None the command line arguments are parsed
        """
        self.exit_code = 1

        self._parse_args(args)

        if not self._args.net and not self._args.file:
            raise InvalidArgument("no interface enabled - finishing")

        self._create_config()

        self._wd = Config.EXECUTOR_WD.get(self._conf)

        self._log_handler = None
        self._job_reporter = None

        self._setup_aux_dir()
        self._setup_logging()

        self._manager = None
        self._receiver = None

        try:
            self._setup_reports()
            QCGPMService._setup_event_loop()

            self._ifaces = []
            if self._args.file:
                iface = FileInterface()
                iface.setup(self._conf)
                self._ifaces.append(iface)

            if self._args.net:
                iface = ZMQInterface()
                iface.setup(self._conf)
                self._ifaces.append(iface)

            if self._args.governor:
                self._setup_governor_manager(self._args.parent)
            else:
                self._setup_direct_manager(self._args.parent)

            self._setup_address_file()
        except Exception:
            if self._log_handler:
                logging.getLogger().removeHandler(self._log_handler)
                self._log_handler = None

            raise

    def _setup_governor_manager(self, parent_manager):
        """Setup QCG-PilotJob manager and governor manager.

        Args:
            parent_manager (str): address of parent manager - currently not supported.
        """
        logging.info('starting governor manager ...')
        self._manager = GovernorManager(self._conf, parent_manager)
        self._manager.register_notifier(self._job_status_change_notify, self._manager)

        self._receiver = Receiver(self._manager.get_handler(), self._ifaces)

    def _setup_direct_manager(self, parent_manager):
        """Setup QCG-PilotJob manager as a single instance or partition manager.

        Args:
            parent_manager (str): if defined the partition manager instance will be created controlled by the
                governor manager with this address
        """
        logging.info('starting direct manager (with parent manager address %s)...', parent_manager)
        self._manager = DirectManager(self._conf, parent_manager)
        self._manager.register_notifier(self._job_status_change_notify, self._manager)

        self._receiver = Receiver(self._manager.get_handler(), self._ifaces)

    def _setup_address_file(self):
        """Write address of ZMQ interface address to the file."""
        if self._receiver.zmq_address:
            address_file = Config.ADDRESS_FILE.get(self._conf)
            address_file = address_file if isabs(address_file) else join(self._aux_dir, address_file)

            if exists(address_file):
                os.remove(address_file)

            with open(address_file, 'w') as address_f:
                address_f.write(self._receiver.zmq_address)

            logging.debug('address interface written to the %s file...', address_file)

    def _setup_reports(self):
        """Setup job report file and proper reporter according to configuration."""
        report_file = Config.REPORT_FILE.get(self._conf)
        job_report_file = report_file if isabs(report_file) else join(self._aux_dir, report_file)

        if exists(job_report_file):
            os.remove(job_report_file)

        self._job_reporter = get_reporter(Config.REPORT_FORMAT.get(self._conf), job_report_file)

    def _setup_aux_dir(self):
        """This method should be called before all other '_setup' methods, as it sets the destination for the
        auxiliary files directory.
        """
        wdir = Config.EXECUTOR_WD.get(self._conf)

        self._aux_dir = join(wdir, '.qcgpjm-service-{}'.format(Config.MANAGER_ID.get(self._conf)))
        if not os.path.exists(self._aux_dir):
            os.makedirs(self._aux_dir)

        self._conf[Config.AUX_DIR] = self._aux_dir

    def _setup_logging(self):
        """Setup logging handlers according to the configuration."""
        log_file = join(self._aux_dir, 'service.log')

        if exists(log_file):
            os.remove(log_file)

        root_logger = logging.getLogger()
        self._log_handler = logging.FileHandler(filename=log_file, mode='a', delay=False)
        self._log_handler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
        root_logger.addHandler(self._log_handler)
        root_logger.setLevel(logging._nameToLevel.get(Config.LOG_LEVEL.get(self._conf).upper()))

        logging.info('service %s version %s started %s @ %s (with tags %s)', Config.MANAGER_ID.get(self._conf),
                     qcg.pilotjob.__version__, str(datetime.now()), socket.gethostname(),
                     ','.join(Config.MANAGER_TAGS.get(self._conf)))
        logging.info('log level set to: %s', Config.LOG_LEVEL.get(self._conf).upper())

        env_file_path = join(self._aux_dir, 'env.log')
        with open(env_file_path, "wt") as env_file:
            for name, value in os.environ.items():
                env_file.write(f'{name}={value}\n')

    @staticmethod
    def _setup_event_loop():
        """Setup event loop."""
#        tasks = asyncio.Task.all_tasks(asyncio.get_event_loop())
#        logging.info('#{} all tasks in event loop before checking for open'.format(len(tasks)))
#        for idx, task in enumerate(tasks):
#            logging.info('\ttask {}: {}'.format(idx, str(task)))
#                asyncio.get_event_loop().run_until_complete(task)

#        tasks = asyncio.Task.current_task(asyncio.get_event_loop())
#        if tasks:
#            logging.info('#{} current tasks in event loop before checking for open'.format(len(tasks)))
#            for idx, task in enumerate(tasks):
#                logging.info('\ttask {}: {}'.format(idx, str(task)))

        logging.debug('checking event loop')
        if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
            logging.debug('setting new event loop')
            asyncio.set_event_loop(asyncio.new_event_loop())

        try:
            import nest_asyncio
            nest_asyncio.apply()
        except ImportError:
            logging.debug('not found nest_asyncio')
        except Exception as exc:
            logging.info(f'not applying nest_asyncio: {str(exc)}')

#        asyncio.get_event_loop().set_debug(True)
#        different child watchers - available in Python >=3.8
#        asyncio.set_child_watcher(asyncio.ThreadedChildWatcher())

    @profile
    async def _stop_interfaces(self, receiver):
        """Asynchronous task working in background waiting for receiver finish flag to finish receiver.
        Before receiver will be stopped, the final status of QCG-PilotJob will be written to the file.

        Args:
              receiver (Receiver): receiver to watch for finish flag and to stop
        """
        while not receiver.is_finished:
            await asyncio.sleep(0.5)

        logging.info('receiver stopped')

        try:
            response = await receiver.generate_status_response()

            status_file = Config.FINAL_STATUS_FILE.get(self._conf)
            status_file = status_file if isabs(status_file) else join(self._aux_dir, status_file)

            if exists(status_file):
                os.remove(status_file)

            with open(status_file, 'w') as status_f:
                status_f.write(response.to_json())
        except Exception as exc:
            logging.warning('failed to write final status: %s', str(exc))

        logging.info('stopping receiver ...')
        await receiver.stop()

    @profile
    def _job_status_change_notify(self, job_id, iteration, state, manager):
        """Callback function called when any job's iteration change it's state.
        The job reporter is called for finished jobs.

        Args:
            job_id (str): job identifier
            iteration (int): iteration index
            state (JobState): new state
            manager (Manager): the manager instance
        """
        if self._job_reporter:
            if state.is_finished():
                self._job_reporter.report_job(manager.job_list.get(job_id), iteration)

    def get_interfaces(self, iface_class=None):
        """Return list of available interfaces.

        Args:
            iface_class (Class) - class of interface, if not defined all configured interfaces are returned.

        Returns:
            list of all or specific input interfaces
        """
        if iface_class:
            return [iface for iface in self._ifaces if isinstance(iface, iface_class)]

        return self._ifaces

    async def _run_service(self):
        """Asynchronous background task that starts receiver, waits for it's finish (signaled by the
        ``_stop_interfaces`` task and clean ups all resources.

        This task can be treatd as the main processing task.
        """
        logging.debug('starting receiver ...')
        self._receiver.run()

        logging.debug('finishing intialization of managers ...')

        try:
            await self._manager.setup_interfaces()

            await self._stop_interfaces(self._receiver)
            self.exit_code = 0
        except Exception:
            logging.error('Service failed: %s', sys.exc_info())
            logging.error(traceback.format_exc())
        finally:
            if self._job_reporter:
                self._job_reporter.flush()

            if self._receiver:
                await self._receiver.stop()

            logging.info('receiver stopped')

            if self._manager:
                await self._manager.stop()

            logging.info('manager stopped')

            usage = QCGPMService.get_rusage()
            logging.info('service resource usage: %s', str(usage.get('service', {})))
            logging.info('jobs resource usage: %s', str(usage.get('jobs', {})))

    @profile
    def start(self):
        """Start QCG-JobManager service.

        The asynchronous task ``_run_service`` is started.
        """
        try:
            asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(self._run_service()))
        finally:
            logging.info('closing event loop')

            tasks = asyncio.Task.all_tasks(asyncio.get_event_loop())
            logging.info('#%d all tasks in event loop before closing', len(tasks))
            for idx, task in enumerate(tasks):
                logging.info('\ttask %d: %s', idx, str(task))
#                asyncio.get_event_loop().run_until_complete(task)

            tasks = asyncio.Task.current_task(asyncio.get_event_loop())
            if tasks:
                logging.info('#%d current tasks in event loop before closing after waiting', len(tasks))
                for idx, task in enumerate(tasks):
                    logging.info('\ttask %d: %s', idx, str(task))

#           asyncio.get_event_loop()._default_executor.shutdown(wait=True)
#           asyncio.get_event_loop().shutdown_asyncgens()
            asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))
            asyncio.get_event_loop().close()
            logging.info('event loop closed')

#           remove custom log handler
            if self._log_handler:
                logging.getLogger().removeHandler(self._log_handler)

    @staticmethod
    def get_rusage():
        """Return resource usage statistics."""
        service_ru = resource.getrusage(resource.RUSAGE_SELF)
        jobs_ru = resource.getrusage(resource.RUSAGE_CHILDREN)

        return {'service': service_ru, 'jobs': jobs_ru}


class QCGPMServiceProcess(Process):
    """This class is used to start QCG-PilotJob manager in separate, background thread.

    Attributes:
        args (list(str)): command line arguments for QCG-PilotJob manager
        queue (multiprocess.Queue): the queue where address of ZMQ interface of QCG-PilotJob manager should be sent
        service (QCGPMService): the service instance
    """

    def __init__(self, args=None, queue=None):
        """Start QCGPM Service as a separate process.

        Args:
            args (str[]) - command line arguments
            queue (Queue) - the communication queue
        """
        super(QCGPMServiceProcess, self).__init__()

        self.args = args or []
        self.queue = queue
        self.service = None

    def run(self):
        """The main thread function.
        The QCG-PilotJob manager service is started, and through multiprocess queue the address of ZMQ interfaces of
        this service is sent to calling thread.
        """
        try:
            print('starting qcgpm service ...')
            self.service = QCGPMService(self.args)

            if self.queue:
                print('communication queue defined ...')
                zmq_ifaces = self.service.get_interfaces(ZMQInterface)
                print('sending configuration through communication queue ...')
                self.queue.put({'zmq_addresses': [str(iface.real_address) for iface in zmq_ifaces]})
            else:
                print('communication queue not defined')

            print('starting qcgpm service inside process ....')
            self.service.start()
        except Exception as exc:
            logging.error('Error: %s', str(exc))
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    try:
        service = QCGPMService()
        service.start()
        sys.exit(service.exit_code)
    except Exception as exc:
        sys.stderr.write('Error: {}\n'.format(str(exc)))
        traceback.print_exc()
        sys.exit(1)
