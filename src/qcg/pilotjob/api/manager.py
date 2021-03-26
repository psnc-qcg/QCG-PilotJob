import re
import json
import os
import time
import logging
import queue
from os.path import exists, join, dirname, abspath

import multiprocessing as mp

import zmq
from qcg.pilotjob import logger as top_logger
from qcg.pilotjob.api import errors
from qcg.pilotjob.api.jobinfo import JobInfo


_logger = logging.getLogger(__name__)


class Manager:
    """The Manager class is used to communicate with single QCG-PilotJob manager instance.

    We assume that QCG-PilotJob manager instance is already running with ZMQ interface. The communication with
    QCG-PilotJob is fully synchronous.
    """

    DEFAULT_ADDRESS_ENV = "QCG_PM_ZMQ_ADDRESS"
    DEFAULT_ADDRESS = "tcp://127.0.0.1:5555"
    DEFAULT_PROTO = "tcp"
    DEFAULT_PORT = "5555"

    DEFAULT_POLL_DELAY = 2

    def __init__(self, address=None, cfg=None):
        """Initialize instance.

        Args:
            address (str) - the address of the PJM manager to connect to in the form: [proto://]host[:port]
              the default values for 'proto' and 'port' are respectively - 'tcp' and '5555'; if 'address'
              is not defined the following procedure will be performed:
                a) if the environment contains QCG_PM_ZMQ_ADDRESS - the value of this var will be used,
                  else
                b) the tcp://127.0.0.1:5555 default address will be used
            cfg (dict) - the configuration; currently the following keys are supported:
              'poll_delay' - the delay between following status polls in wait methods
              'log_file' - the location of the log file
              'log_level' - the log level ('DEBUG'); by default the log level is set to INFO
        """
        self._zmq_ctx = zmq.Context()
        self._zmq_socket = None
        self._connected = False

        self._poll_delay = Manager.DEFAULT_POLL_DELAY

        self._log_handler = None

        client_cfg = cfg or {}
        self._setup_logging(client_cfg)

        if address is None:
            if Manager.DEFAULT_ADDRESS_ENV in os.environ:
                address = os.environ[Manager.DEFAULT_ADDRESS_ENV]
                _logger.debug('found zmq address of pm: %s', address)
            else:
                address = Manager.DEFAULT_ADDRESS

        if 'poll_delay' in client_cfg:
            self._poll_delay = client_cfg['poll_delay']

        self._address = Manager._parse_address(address)
        self._connect()

    @staticmethod
    def _parse_address(address):
        """Validate the QCG PJM address.

        Args:
            address (str): the address to validate; if address is not in the complete form, it will be
              extended with the default values (protocol, port)

        Returns:
            str: validated address
        """
        if not re.match(r'\w*://', address):
            # append default protocol
            address = "%s://%s" % (Manager.DEFAULT_PROTO, address)

        if not re.match(r'.*:\d+', address):
            # append default port
            address = "%s:%s" % (address, Manager.DEFAULT_PORT)

        return address

    def _setup_logging(self, cfg):
        """Setup the logging.

        The log file and the log level are set.

        Args:
            cfg (dict): see ``__init__``
        """
        wdir = cfg.get('wdir', '.')
        _log_file = cfg.get('log_file', join(wdir, '.qcgpjm-client', 'api.log'))

        if not exists(dirname(abspath(_log_file))):
            os.makedirs(dirname(abspath(_log_file)))
        elif exists(_log_file):
            os.remove(_log_file)

        self._log_handler = logging.FileHandler(filename=_log_file, mode='a', delay=False)
        self._log_handler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
        top_logger.addHandler(self._log_handler)

        level = logging.INFO
        if 'log_level' in cfg:
            if cfg['log_level'].lower() == 'debug':
                level = logging.DEBUG

        top_logger.setLevel(level)

    def _disconnect(self):
        """Close connection to the QCG-PJM

        Raises:
            ConnectionError: if there was an error during closing the connection.
        """
        try:
            if self._connected:
                self._zmq_socket.close()
                self._connected = False
        except Exception as exc:
            raise errors.ConnectionError('Failed to disconnect {}'.format(exc.args[0]))

    def _connect(self):
        """Connect to the QCG-PJM.

        The connection is made to the address defined in the constructor. The success of this method is does not mean
        that communication with QCG-PilotJob manager instance has been established, as in case of ZMQ communication,
        only when sending and receiving messages the real communication takes place.

        Raises:
            ConnectionError: in case of error during establishing connection.
        """
        self._disconnect()

        _logger.info("connecting to the PJM @ %s", self._address)
        try:
            self._zmq_socket = self._zmq_ctx.socket(zmq.REQ)  # pylint: disable=maybe-no-member
            self._zmq_socket.connect(self._address)
            self._connected = True
            _logger.info("connection established")
        except Exception as exc:
            raise errors.ConnectionError('Failed to connect to {} - {}'.format(self._address, exc.args[0]))

    def _assure_connected(self):
        """Check if connection has been successfully opened.

        Raises:
            ConnectionError: if connection has not been established yet
        """
        if not self._connected:
            raise errors.ConnectionError('Not connected')

    @staticmethod
    def _validate_response(response):
        """Validate the response from the QCG PJM.

        This method checks the format of the response and exit code.

        Args:
            response (dict): deserialized JSON response

        Returns:
            dict: validated response data

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code
        """
        if not isinstance(response, dict) or 'code' not in response:
            raise errors.InternalError('Invalid reply from the service')

        if response['code'] != 0:
            if 'message' in response:
                raise errors.ConnectionError('Request failed - {}'.format(response['message']))

            raise errors.ConnectionError('Request failed')

        if 'data' not in response:
            raise errors.InternalError('Invalid reply from the service')

        return response['data']

    @staticmethod
    def _validate_response_wo_data(response):
        """Validate the response from the QCG PJM.

        This method checks the format of the response and exit code.
        Unlike the ``_validate_seponse`` this method not checks existence of the 'data' element and returns the full
        reponse.

        Args:
            response (dict): deserialized JSON response

        Returns:
            dict: response message

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code
        """
        if not isinstance(response, dict) or 'code' not in response:
            raise errors.InternalError('Invalid reply from the service')

        if response['code'] != 0:
            if 'message' in response:
                raise errors.ConnectionError('Request failed - %s' % response['message'])

            raise errors.ConnectionError('Request failed')

        return response

    def send_request(self, request):
        """Method for testing purposes - allows to send any request to the QCG PJM.
        The received response is validated for correct format.

        Args:
            request (dict): the request data to send

        Returns:
            dict: validated response
        """
        return self._send_and_validate_result(request, valid_method=Manager._validate_response_wo_data)

    def _send_and_validate_result(self, data, valid_method=None):
        """Send synchronously request to the QCG-PJM and validate response.

        The input data is encoded to the JSON format and send to the QCG PJM. After receiving, response is validated.

        Args:
            data (dict): the request data to send
            valid_method (def): the method should be used to validate response

        Returns:
            data (dict): received data from the QCG PJM service

        Raises:
            see _assure_connected, _validate_response
        """
        if not valid_method:
            valid_method = Manager._validate_response

        self._assure_connected()

        msg = str.encode(json.dumps(data))
        _logger.debug("sending (in process %d): %s", os.getpid(), msg)
        self._zmq_socket.send(msg)
        _logger.debug("data send, waiting for response")

        reply = bytes.decode(self._zmq_socket.recv())

        _logger.debug("got reply: %s", str(reply))
        return valid_method(json.loads(reply))

    def resources(self):
        """Return available resources.

        Return information about current resource status of QCG PJM.

        Returns:
            dict: data in format described in 'resourceInfo' method of QCG PJM.

        Raises:
            see _send_and_validate_result
        """
        return self._send_and_validate_result({
            "request": "resourcesInfo"
        })

    def submit(self, jobs):
        """Submit jobs.

        Args:
            jobs (Jobs): the job descriptions to submit

        Returns:
            list(str): list of submitted job names

        Raises:
            InternalError - in case of unexpected result format
            see _send_and_validate_result
        """
        data = self._send_and_validate_result({
            "request": "submit",
            "jobs": jobs.ordered_jobs()
        })

        if 'submitted' not in data or 'jobs' not in data:
            raise errors.InternalError('Missing response data')

        return data['jobs']

    def list(self):
        """List all jobs.

        Return a list of all job names registered in the QCG PJM. Beside the name, each job will contain additional
        data, like:
            status (str) - current job status
            messages (str, optional) - error message generated during job processing
            inQueue (int, optional) - current job position in scheduling queue

        Returns:
            dict: dictionary with job names and attributes

        Raises:
            InternalError - in case of unexpected result format
            see _send_and_validate_result
        """
        data = self._send_and_validate_result({
            "request": "listJobs"
        })

        if 'jobs' not in data:
            raise errors.InternalError('Rquest failed - missing jobs data')

        return data['jobs']

    def status(self, names):
        """Return current status of jobs.

        Args:
            names (str|list(str)): list of job names to get status for

        Returns:
            dict: dictionary with job names and status data in format of dictionary with following keys:
                status (int): 0 - job found, other value - job not found
                message (str): an error description
                data (dict):
                    jobName: job name
                    status: current job status

        Raises:
            see _send_and_validate_result
        """
        if isinstance(names, str):
            job_names = [names]
        else:
            job_names = list(names)

        return self._send_and_validate_result({
            "request": "jobStatus",
            "jobNames": job_names
        })

    def info(self, names, **kwargs):
        """Return detailed information about jobs.

        Args:
            names (str|list(str)): list of job names to get detailed information about
            kwargs (**dict): additional keyword arguments to the info method, currently following attributes are
                supported:
                    withChilds (bool): if True the detailed information about all job's iterations will be returned

        Returns:
            dict: dictionary with job names and detailed information in format of dictionary with following keys:
                status (int): 0 - job found, other value - job not found
                message (str): an error description
                data (dict):
                    jobName (str): job name
                    status (str): current job status
                    iterations (dict, optional): the information about iteration job
                        start: start index of iterations
                        stop: stop index of iterations
                        total: total number of iterations
                        finished: already finished number of iterations
                        failed: already failed number of iterations
                    childs (list(dict), optional): only when 'withChilds' option has been used, each entry contains:
                        iteration (int): the iteration index
                        state (str): current state of iteration
                        runtime (dict): runtime information
                    messages (str, optional): error description
                    runtime (dict, optional): runtime information, see below
                    history (str): history of status changes, see below

            The runtime information can contains following keys:
                allocation (str): information about allocated resources in form:
                        NODE_NAME0[CORE_ID0[:CORE_ID1+]][,NODE_NAME1[CORE_ID0[:CORE_ID1+]].....]
                    the nodes are separated by the comma, and each node contain CPU's identifiers separated by colon :
                    enclosed in square brackets
                wd (str): path to the working directory
                rtime (str): the running time (set at the job's or job's iteration finish)
                exit_code (int): the exit code (set at the job's or job's iteration finish)

            The history information contains multiple lines, where each line has format:
                YEAR-MONTH-DAY HOUR:MINUTE:SECOND.MILLIS: STATE
            The first part is a job's or job's iteration status change timestamp, and second is the new state.

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        if isinstance(names, str):
            job_names = [names]
        else:
            job_names = list(names)

        req = {
            "request": "jobInfo",
            "jobNames": job_names
        }
        if kwargs:
            req["params"] = kwargs

        return self._send_and_validate_result(req)

    def info_parsed(self, names, **kwargs):
        """Return detailed and parsed information about jobs.

        The request sent to the QCG-PilotJob manager instance is the same as in ``info``, but the result information is
        parsed into more simpler to use ``JobInfo`` object.

        Args:
            names (str|list(str)): list of job names to get detailed information about
            kwargs (**dict): additional keyword arguments to the info method, currently following attributes are
                supported:
                    withChilds (bool): if True the detailed information about all job's iterations will be returned

        Returns:
            dict(str, JobInfo): a dictionary with job names and information parsed into JobInfo object

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        return {jname: JobInfo.from_job(jinfo.get('data', {}))
                for jname, jinfo in self.info(names, **kwargs).get('jobs', {}).items()}

    def remove(self, names):
        """Remove jobs from QCG-PilotJob manager instance.

        This function might be useful if we want to submit jobs with the same names as previously used, or to release
        memory allocated for storing information about already finished jobs. After removing, there will be not possible
        to get any information about removed jobs.

        Args:
            names (str|list(str)): list of job names to remove from QCG-PilotJob manager

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        if isinstance(names, str):
            job_names = [names]
        else:
            job_names = list(names)

        self._send_and_validate_result({
            "request": "removeJob",
            "jobNames": job_names
        })

    def cancel(self, names):
        """Cancel jobs execution.

        This method is currently not supported.

        Args:
            names (str|list(str)): list of job names to cancel

        Raises:
            InternalError: always
        """
        if isinstance(names, str):
            job_names = [names]
        else:
            job_names = list(names)

        self._send_and_validate_result({
            "request": "cancelJob",
            "jobNames": job_names
        })

    def _send_finish(self):
        """Send finish request to the QCG-PilotJob manager, close connection.

        Sending finish request to the QCG-PilotJob manager result in closing instance of QCG-PilotJob manager (with
        some delay). There will be not possible to send any new requests to this instance of QCG-PilotJob manager.

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        self._assure_connected()

        self._zmq_socket.send(str.encode(json.dumps({
            "request": "finish"
        })))

        reply = bytes.decode(self._zmq_socket.recv())
        Manager._validate_response(json.loads(reply))

        self._disconnect()

    def finish(self):
        """Send finish request to the QCG-PilotJob manager, close connection.

        Sending finish request to the QCG-PilotJob manager result in closing instance of QCG-PilotJob manager (with
        some delay). There will be not possible to send any new requests to this instance of QCG-PilotJob manager.

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        self._send_finish()
        self.cleanup()

    def cleanup(self):
        """Clean up resources.

        The custom logging handlers are removed from top logger.
        """
        if self._log_handler:
            self._log_handler.close()
            top_logger.removeHandler(self._log_handler)

    def wait4(self, names):
        """Wait for finish of specific jobs.

        This method waits until all specified jobs finish its execution (successfully or not).
        The QCG-PilotJob manager is periodically polled about status of not finished jobs. The poll interval (2 sec by
        default) can be changed by defining a 'poll_delay' key with appropriate value (in seconds) in
        configuration of instance.

        Args:
            names (str|list(str)): list of job names to get detailed information about

        Returns:
            dict - a map with job names and their terminal status

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        if isinstance(names, str):
            job_names = [names]
        else:
            job_names = list(names)

        _logger.info("waiting for finish of %d jobs", len(job_names))

        result = {}
        not_finished = job_names
        while len(not_finished) > 0:
            try:
                jobs_status = self.status(not_finished)

                not_finished = []
                for job_name, job_data in jobs_status['jobs'].items():
                    if 'status' not in job_data['data'] or job_data['status'] != 0 or 'data' not in job_data:
                        raise errors.InternalError("Missing job's {} data".format(job_name))

                    if not Manager.is_status_finished(job_data['data']['status']):
                        not_finished.append(job_name)
                    else:
                        result[job_name] = job_data['data']['status']

                if len(not_finished) > 0:
                    _logger.info("still %d jobs not finished", len(not_finished))
                    time.sleep(self._poll_delay)

            except Exception as exc:
                raise errors.ConnectionError(exc.args[0])

        _logger.info("all jobs finished")
        return result

    def wait4all(self):
        """Wait for finish of all submitted jobs.

        This method waits until all jobs submitted to service finish its execution (successfully or not).
        """
        not_finished = True
        while not_finished:
            status = self._send_and_validate_result({
                "request": "status",
                "options": { "allJobsFinished": True }
            })
            not_finished = status.get("AllJobsFinished", False) is False
            if not_finished:
                time.sleep(self._poll_delay)

        _logger.info("all jobs finished in manager")

    @staticmethod
    def is_status_finished(status):
        """Check if status of a job is a terminal status.

        Args:
            status (str):  a job status

        Returns:
            bool: true if a given status is a terminal status
        """
        return status in ['SUCCEED', 'FAILED', 'CANCELED', 'OMITTED']


class LocalManager(Manager):
    """The Manager class which launches locally (in separate thread) instance of QCG-PilotJob manager

    The communication model as all functionality is the same as in ``Manager`` class.
    """

    def __init__(self, server_args=None, cfg=None):
        """Initialize instance.

        Launch QCG-PilotJob manager instance in background thread and connect to it. The port number for ZMQ interface
        of QCG-PilotJob manager instance is randomly selected.

        Args:
            server_args (list(str)): the command line arguments for QCG-PilotJob manager instance

                  --net                 enable network interface
                  --net-port NET_PORT   port to listen for network interface (implies --net)
                  --net-port-min NET_PORT_MIN
                                        minimum port range to listen for network interface if
                                        exact port number is not defined (implies --net)
                  --net-port-max NET_PORT_MAX
                                        maximum port range to listen for network interface if
                                        exact port number is not defined (implies --net)
                  --file                enable file interface
                  --file-path FILE_PATH
                                        path to the request file (implies --file)
                  --wd WD               working directory for the service
                  --envschema ENVSCHEMA
                                        job environment schema [auto|slurm]
                  --resources RESOURCES
                                        source of information about available resources
                                        [auto|slurm|local] as well as a method of job
                                        execution (through local processes or as a Slurm sub
                                        jobs)
                  --report-format REPORT_FORMAT
                                        format of job report file [text|json]
                  --report-file REPORT_FILE
                                        name of the job report file
                  --nodes NODES         configuration of available resources (implies
                                        --resources local)
                  --log {critical,error,warning,info,debug,notset}
                                        log level
                  --system-core         reserve one of the core for the QCG-PJM
                  --disable-nl          disable custom launching method
                  --show-progress       print information about executing tasks
                  --governor            run manager in the governor mode, where jobs will be
                                        scheduled to execute to the dependant managers
                  --parent PARENT       address of the parent manager, current instance will
                                        receive jobs from the parent manaqger
                  --id ID               optional manager instance identifier - will be
                                        generated automatically when not defined
                  --tags TAGS           optional manager instance tags separated by commas
                  --slurm-partition-nodes SLURM_PARTITION_NODES
                                        split Slurm allocation by given number of nodes, where
                                        each group will be controlled by separate manager
                                        (implies --governor)
                  --slurm-limit-nodes-range-begin SLURM_LIMIT_NODES_RANGE_BEGIN
                                        limit Slurm allocation to specified range of nodes
                                        (starting node)
                  --slurm-limit-nodes-range-end SLURM_LIMIT_NODES_RANGE_END
                                        limit Slurm allocation to specified range of nodes
                                        (ending node)

                each command line argument and (optionaly) it's value should be passed as separate entry in the list

            cfg (dict) - the configuration; currently the following keys are supported:
              'poll_delay' - the delay between following status polls in wait methods
              'log_file' - the location of the log file
              'log_level' - the log level ('DEBUG'); by default the log level is set to INFO
        """
        _logger.debug('initializing MP start method with "spawn"')
        mp.set_start_method("fork", force=True)
        mp.freeze_support()
                        
        try:
            from qcg.pilotjob.service import QCGPMServiceProcess
        except ImportError:
            raise errors.ServiceError('qcg.pilotjob library is not available')

        if not server_args:
            server_args = ['--net']
        elif '--net' not in server_args:
            server_args.append('--net')

        server_args = [str(arg) for arg in server_args]

        self.qcgpm_queue = mp.Queue()
        self.qcgpm_process = QCGPMServiceProcess(server_args, self.qcgpm_queue)
        self.qcgpm_conf = None
        _logger.debug('manager process created')

        self.qcgpm_process.start()
        _logger.debug('manager process started')

        for i in range(1, 20):
            if not self.qcgpm_process.is_alive():
                raise errors.ServiceError('Service not started')

            try:
                self.qcgpm_conf = self.qcgpm_queue.get(block=True, timeout=3)
                break
            except queue.Empty:
                continue
#                raise errors.ServiceError('Service not started - timeout')
            except Exception as exc:
                raise errors.ServiceError('Service not started: {}'.format(str(exc)))

        if not self.qcgpm_conf:
            raise errors.ServiceError('Service not started')

        if self.qcgpm_conf.get('error', None):
            raise errors.ServiceError(self.qcgpm_conf['error'])

        _logger.debug('got manager configuration: %s', str(self.qcgpm_conf))
        if not self.qcgpm_conf.get('zmq_addresses', None):
            raise errors.ConnectionError('Missing QCGPM network interface address')

        zmq_iface_address = self.qcgpm_conf['zmq_addresses'][0]
        _logger.info('manager zmq iface address: %s', zmq_iface_address)

        super(LocalManager, self).__init__(zmq_iface_address, cfg)

    def finish(self):
        """Send a finish control message to the manager and stop the manager's process.

        Sending finish request to the QCG-PilotJob manager result in closing instance of QCG-PilotJob manager (with
        some delay). There will be not possible to send any new requests to this instance of QCG-PilotJob manager.

        If the manager process won't stop in 10 seconds it will be terminated.
        We also call the 'cleanup' method.

        Raises:
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        super(LocalManager, self)._send_finish()

        self.qcgpm_process.join(10)
        self.kill_manager_process()

        super(LocalManager, self).cleanup()

    def kill_manager_process(self):
        """Terminate the manager's process with the SIGTERM signal.

        In normal conditions the ``finish`` method should be called.
        """
        self.qcgpm_process.terminate()
