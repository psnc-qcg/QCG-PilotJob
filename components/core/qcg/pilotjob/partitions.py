import asyncio
import copy
import getpass
import logging
import math
import os
import shutil
import socket
import sys
import traceback
from datetime import datetime

import zmq

from qcg.pilotjob.logger import top_logger
from qcg.pilotjob.config import Config
from qcg.pilotjob.errors import InvalidRequest, JobAlreadyExist, InternalError
from qcg.pilotjob.joblist import JobState
from qcg.pilotjob.request import ControlReq
from qcg.pilotjob.resources import Resources
from qcg.pilotjob.response import Response, ResponseCode
from qcg.pilotjob.slurmres import in_slurm_allocation, parse_slurm_resources
from qcg.pilotjob.utils.processes import terminate_subprocess


_logger = logging.getLogger(__name__)


class GlobalJob:
    """Job information stored at GovernorManager

    Attributes:
        name (str): job name
        status (JobStatus): current job status
        job_parts (list(GlobalJobPart)): instances of the job parts
        unfinished (int): number of unfinished job parts
    """

    def __init__(self, name, status, job_parts):
        """Initialize job information

        Args:
            name (str):  job name
            status (JobStatus): current job status
            job_parts (GlobalJobPart[]): instances of the job parts
       """
        self.name = name
        self.status = status

        self.job_parts = {job_part.pid: job_part for job_part in job_parts}
        self.unfinished = len(self.job_parts)

    def __str__(self):
        """str: return string representation of GlobalJob"""
        return '{} in {} state (with {} managers)'.format(self.name, self.status.name, len(self.job_parts))

    def update_part_status(self, part_id, state):
        """
        Set new job part state.
        In this method we compute also the new global job state.

        Args:
            part_id (str): identifier of job part
            state (JobState): current job status

        Returns:
            bool: True if status of the whole job has been updated
        """
        job_part = self.job_parts.get(part_id)
        if not job_part:
            return False

        job_part.state = state
        if state.is_finished():
            # updating global status
            self.unfinished = self.unfinished - 1
            if self.unfinished == 0:
                nfailed_parts = sum([1 if job_part.state in [JobState.FAILED, JobState.OMITTED, JobState.CANCELED]
                                     else 0 for job_part in self.job_parts.values()])
                self.status = JobState.SUCCEED if nfailed_parts == 0 else JobState.FAILED

        return True


class GlobalJobPart:
    """Information about part of the global iterated job.

    Attributes:
        id (str): job part identifier
        local_job_name (str): name of the job in the local manager instance
        it_start (int): iteration start (None in case of noniterated job)
        it_stop (int): iteration stop (None in case of noniterated job)
        status (JobStatus): status of this part of job
        manager_instance (ManagerInstance): address of the manager instance that schedules & executes part of the job
     """

    def __init__(self, pid, local_job_name, it_start, it_stop, status, manager_instance):
        """Initialize information about part of the global iterated job.

        Args:
            pid (str): job part identifier
            local_job_name (str): name of the job in the local manager instance
            it_start (int): iteration start (None in case of noniterated job)
            it_stop (int): iteration stop (None in case of noniterated job)
            status (JobStatus): status of this part of job
            manager_instance (ManagerInstance): address of the manager instance that schedules & executes part of the
                job
        """
        self.pid = pid
        self.local_job_name = local_job_name
        self.it_start = it_start
        self.it_stop = it_stop
        self.status = status
        self.manager_instance = manager_instance


class PartitionManager:
    """
    The instance of partition manager.
    The partition manager is an instance of manager that controls part of the allocation (the nodes range
    'start_node'-'end_node' of the Slurm allocation). The instance is launched from governor manager to run
    jobs submited by governor on it's part of the allocation.

    Attributes:
        mid (str): partition manager identifier
        node_name (str): the node where partition manager should be launched
        start_node (int): the start node of the allocation the manager should control
        end_node (int): the end node of the allocation the manager should control
        work_dir (str): the working directory of partition manager
        governor_address (str): the address of the governor manager where partition manager should register
        aux_dir (str): the auxiliary directory for partition manager
        config (dict): the service configuration
        stdout_p (subprocess pipe): the standard output of launched instance of partition manager
        stderr_p (subprocess pipe): the standard error of launched instance of partition manager
        process (subprocess): the process of launched instance of partiton manager
        _partition_manager_cmd (list(str)): the command parts to launch instance of partition manager
        _default_partition_manager_args (list(str)): additional arguments to launch instance of partition manager
     """

    def __init__(self, mid, node_name, start_node, end_node, work_dir, governor_address, aux_dir, config):
        """Initialize partition manager.

        Args:
            mid (str): partition manager identifier
            node_name (str): the node where partition manager should be launched
            start_node (int): the start node of the allocation the manager should control
            end_node (int): the end node of the allocation the manager should control
            work_dir (str): the working directory of partition manager
            governor_address (str): the address of the governor manager where partition manager should register
            aux_dir (str): the auxiliary directory for partition manager
            config (dict): the service configuration
        """
        self.mid = mid
        self.node_name = node_name
        self.start_node = start_node
        self.end_node = end_node
        self.work_dir = work_dir
        self.governor_address = governor_address
        self.aux_dir = aux_dir
        self.config = config

        self.stdout_p = None
        self.stderr_p = None

        self.process = None

        self._partition_manager_cmd = [sys.executable, '-m', 'qcg.pilotjob.service']

    async def launch(self):
        """Launch instance of partition manager."""

        _logger.info('launching partition manager %s on node %s to control node numbers %d-%d',
                     self.mid, self.node_name, self.start_node, self.end_node)

        try:
            slurm_args = ['-J', str(self.mid), '-w', self.node_name, '--oversubscribe', '--overcommit',
                          '-N', '1', '-n', '1', '-D', self.work_dir]

            manager_args = ['--id', self.mid, '--parent', self.governor_address,
                            '--slurm-limit-nodes-range-begin', str(self.start_node),
                            '--slurm-limit-nodes-range-end', str(self.end_node)]

            for arg in [Config.ZMQ_PORT_MIN_RANGE, Config.ZMQ_PORT_MAX_RANGE, Config.REPORT_FORMAT,
                    Config.LOG_LEVEL, Config.WRAPPER_RT_STATS, Config.NL_READY_TRESHOLD, Config.NL_INIT_TIMEOUT]:
                if self.config.get(arg, None) is not None:
                    manager_args.extend([arg.value['cmd_opt'], str(self.config.get(arg))])

            for arg in [Config.SYSTEM_CORE, Config.DISABLE_NL, Config.ENABLE_PROC_STATS, Config.ENABLE_RT_STATS]:
                if self.config.get(arg, False):
                    manager_args.append(arg.value['cmd_opt'])

            self.stdout_p = asyncio.subprocess.DEVNULL
            self.stderr_p = asyncio.subprocess.DEVNULL

            if top_logger.level == logging.DEBUG:
                self.stdout_p = open(os.path.join(self.aux_dir, 'part-manager-{}-stdout.log'.format(self.mid)), 'w')
                self.stderr_p = open(os.path.join(self.aux_dir, 'part-manager-{}-stderr.log'.format(self.mid)), 'w')

            _logger.debug('running partition manager process with args: %s',
                          ' '.join([shutil.which('srun')] + slurm_args + self._partition_manager_cmd + manager_args))
        except Exception as exc:
            _logger.error(f'failed to start partition manager: {str(exc)}')
            raise

        self.process = await asyncio.create_subprocess_exec(
            shutil.which('srun'), *slurm_args, *self._partition_manager_cmd, *manager_args,
            stdout=self.stdout_p, stderr=self.stderr_p)

    async def terminate(self):
        """Terminate partition manager instance.

        Wait for 5 second for partition manager finish, and if not finished send SIGKILL
        """
        if self.process:
            try:
                _logger.info(f'terminating partition manager {self.mid} @ {self.start_node}')
                await terminate_subprocess(self.process, f'partition-manager-{self.mid}', 30)
                _logger.info(f'partition manager {self.mid} @ {self.start_node} terminated')
            except Exception as exc:
                _logger.warning(f'failed to terminate partition manager {self.mid} @ {self.start_node}: {str(exc)}')


class ManagerInstance:
    """The information about QCG manager instance available for higher level manager.

    Attributes:
        id (str): manager identifier
        resources (Resources): manager's last reported available resource information
        address (str): manager's ZMQ input interface address
        submitted_jobs (int): number of already submited jobs to this instance
        _socket (zmq.Socket): ZMQ socket used for communication with this instance
    """

    def __init__(self, mid, resources, address):
        """
        The information about QCG manager instance available for higher level manager.

        Args:
            mid (str): manager identifier
            resources (Resources): manager's last reported resources
            address (str): manager's ZMQ input interface address
        """
        self.mid = mid
        self.resources = resources
        self.address = address

        self.submitted_jobs = 0
        self._socket = None

    def stop(self):
        """Cleanup communication resources."""
        self.close_socket()

    def get_socket(self):
        """Return ZMQ socket connected to manager instance.

        The ZMQ socket is created at the first communication.

        Returns:
            zmq.Socket: created socket
        """
        if not self._socket:
            self._socket = zmq.asyncio.Context.instance().socket(zmq.REQ)
            self._socket.connect(self.address)

        return self._socket

    def close_socket(self):
        """Close ZMQ socket."""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                _logger.warning('failed to close manager %s socket: %s', self.address, sys.exc_info())
            self._socket = None

    async def submit_jobs(self, req):
        """Send submit request to the manager instance.

        Args:
            req: the submit request to send

        Returns:
            dict: with:
               njobs (int): number of submitted jobs
               names (list(str)): list of submited job names

        Raises:
            InvalidRequest: when request has wrong format
        """
        await self.get_socket().send_json({
            'request': 'submit',
            'jobs': req
        })

        msg = await self.get_socket().recv_json()
        if not msg['code'] == 0:
            raise InvalidRequest(
                'Failed to submit jobs: {}'.format(msg.get('message', '')))

        self.submitted_jobs += msg.get('data', {}).get('submitted', 0)
        return {'njobs': msg.get('data', {}).get('submitted', 0),
                'names': msg.get('data', {}).get('jobs', [])}

    async def finish(self):
        """Send submit request to the manager instance.

        Args:
            req: the submit request to send

        Returns:
            dict: with:
               njobs (int): number of submitted jobs
               names (list(str)): list of submited job names

        Raises:
            InvalidRequest: when request has wrong format
        """
        _logger.info(f'sending finish request to partition manager {self.mid}')
        try:
            await asyncio.wait_for(self.get_socket().send_json({
                'request': 'control',
                'command': ControlReq.REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE
                }), timeout=10.0)

            msg = await asyncio.wait_for(self.get_socket().recv_json(), timeout=10.0)

            if not msg['code'] == 0:
                raise InvalidRequest(
                    'Failed to finish manager instance: {}'.format(msg.get('message', '')))
        except asyncio.TimeoutError:
            _logger.warning(f'failed to send finish command to partition manager {self.mid}: timeout')



class TotalResources:
    """The information about total resources reported by all partition manager instances.

    Attributes:
        total_nodes (int): number of nodes
        total_cores (int): number of cores
        used_cores (int): number of used cores
        free_cores (int): number of free cores
    """

    def __init__(self):
        """Initialize total resources."""
        self.total_nodes = 0
        self.total_cores = 0
        self.used_cores = 0
        self.free_cores = 0

    def zero(self):
        """Zero all values."""
        self.total_nodes = 0
        self.total_cores = 0
        self.used_cores = 0
        self.free_cores = 0

    def append(self, instance_resources):
        """Append instance resources to the global pool.

        All partition manager instances must be launched on separate nodes, so there will be no shared nodes.

        Args:
            instance_resources (Resources): partition manager instance available resources
        """
        self.total_nodes += instance_resources.total_nodes
        self.total_cores += instance_resources.total_cores
        self.used_cores += instance_resources.used_cores
        self.free_cores += instance_resources.free_cores


class GovernorManager:
    """Instance of QCG-PilotJob manager that controls partition manager instances.

    Manager of jobs to execution but without direct access to the resources. The GovernorManager schedules jobs to
    other, dependant managers.

    Attributes:
        managers (dict): a map with available instances of partition managers
        total_resources (TotalResources): information about all resources reported by the partition managers
        jobs (dict): a map with all submited jobs
        _finish_task (asyncio.Future): task launched by the ``finishAfterAllTasksDone`` control command request,
            that wait until all jobs finish and finish the receiver
        _receiver (Receiver): an instance of receiver object
        zmq_address (str): address of ZMQ input interface
        _config (dict): the QCG-PilotJob configuration
        _create_partitions (int): if defined the partition managers will control given number of nodes each
        _parent_manager (Manager): the instance of the parent governor manager - currently not supported
        _partition_managers (list(PartitionManager)): list of partition managers
        _min_scheduling_managers (int): number of registered partition managers at which job will be submitted, until
            minimum partition manager not register, all incoming submit requests will buffered
        _max_buffered_jobs (int): the maximum number of buffered jobs until, when reached the submit request will be
            responsed with error
        _submit_reqs_buffer (list(Request)): the buffer for submitted jobs until they will be sent to partition managers
        _schedule_buffered_jobs_task (asyncio.Future): the task that will schedule buffered submit requests when minimum
            required number of managers registers
        _wait_for_register_timeout (int): the maximum number of seconds after start to wait for partition managers to
            register
        start_time (DateTime): moment of the governor manager start
        """

    def __init__(self, config=None, parent_manager=None):
        """Initialize governor manager.

        Args:
            config (dict): QCG-PilotJob governor configuration
            parent_manager (Manager): the top level manager - currently not supported
        """
        self.managers = dict()
        self.total_resources = TotalResources()
        self.jobs = dict()

        self._finish_task = None

        self._receiver = None
        self.zmq_address = None

        self._config = config or {}
        self._create_partitions = Config.SLURM_PARTITION_NODES.get(self._config)

        self._parent_manager = parent_manager
        self._partition_managers = []

        self._min_scheduling_managers = 0

        self._max_buffered_jobs = 1000

        self._submit_reqs_buffer = []

        self._schedule_buffered_jobs_task = None

        self._wait_for_register_timeout = 60

        self.start_time = datetime.now()

        self.stop_processing = False


    async def setup_interfaces(self):
        """If defined, launch partition managers."""
        if self._parent_manager:
            try:
                await self._register_in_parent()
            except Exception:
                _logger.error('Failed to register manager in parent governor manager: %s', sys.exc_info()[0])
                raise

        if self._create_partitions:
            await self._launch_partition_managers(self._create_partitions)

    async def _register_in_parent(self):
        """Register governor manager in parent instances - currenlty not supported"""
        _logger.error('Governing managers can not currenlty register in parent managers')
        raise InternalError('Governing managers can not currenlty register in parent managers')

    async def _launch_partition_managers(self, nodes_in_partition):
        """Launch partition managers.

        The information about Slurm allocation is gathered, and all nodes are split by ``nodes_in_partition`` to form
        a single partition, and partition manager instance is created to control each partition.

        Args:
            nodes_in_partition (int): how many nodes each partition should have

        Raises:
            InvalidRequest: when
                * ``nodes_in_partition`` is less than 1
                * governor manager has not been launched in Slurm allocation
                * missing nodes in Slurm allocation
            InternalError: when
                * missing ZMQ interface in governor manager
        """
        _logger.info('setup allocation split into partitions by %s nodes', nodes_in_partition)

        if nodes_in_partition < 1:
            _logger.error('Failed to partition resources - partition size must be greater or equal 1')
            raise InvalidRequest('Failed to partition resources - partition size must be greater or equal 1')

        # get slurm resources - currently only slurm is supported
        if not in_slurm_allocation():
            _logger.error('Failed to partition resources - partitioning resources is currently available only within '
                          'slurm allocation')
            raise InvalidRequest('Failed to partition resources - partitioning resources is currently available only '
                                 'within slurm allocation')

        if not self.zmq_address:
            _logger.error('Failed to partition resources - missing zmq interface address')
            raise InternalError('Failed to partition resources - missing zmq interface address')

        slurm_resources = parse_slurm_resources(self._config)

        if slurm_resources.total_nodes < 1:
            raise InvalidRequest('Failed to partition resources - allocation contains no nodes')

        npartitions = math.ceil(slurm_resources.total_nodes / nodes_in_partition)
        _logger.info('%s partitions will be created (in allocation containing %s total nodes)', npartitions,
                     slurm_resources.total_nodes)

        # launch partition manager in the same directory as governor
        partition_manager_wdir = Config.EXECUTOR_WD.get(self._config)
        partition_manager_auxdir = Config.AUX_DIR.get(self._config)

        _logger.debug('partition managers working directory %s', partition_manager_wdir)

        for part_idx in range(npartitions):
            _logger.debug('creating partition manager %s configuration', part_idx)

            part_node = slurm_resources.nodes[part_idx * nodes_in_partition]

            _logger.debug('partition manager node %s', part_node.name)

            self._partition_managers.append(
                PartitionManager('partition-{}'.format(part_idx), part_node.name, part_idx * nodes_in_partition,
                                 min(part_idx * nodes_in_partition + nodes_in_partition, slurm_resources.total_nodes),
                                 partition_manager_wdir, self.zmq_address, partition_manager_auxdir, self._config))

            _logger.debug('partition manager %s configuration created', part_idx)

        self._min_scheduling_managers = len(self._partition_managers)

        _logger.info('created partition managers configuration and set minimum scheduling managers to %s',
                     self._min_scheduling_managers)

        asyncio.ensure_future(self.manage_start_partition_managers())

        # launch task in the background that schedule buffered submit requests
        self._schedule_buffered_jobs_task = asyncio.ensure_future(self._schedule_buffered_jobs())

    async def manage_start_partition_managers(self):
        """Start and manage partitions manager.

        Ensure that all started partitions manager registered in governor.
        """
        try:
            # start all partition managers
            try:
                _logger.info('starting all partition managers ...')
                await asyncio.gather(*[manager.launch() for manager in self._partition_managers])
            except Exception:
                _logger.error('one of partition manager failed to start - killing the rest')
                # if one of partition manager didn't start - stop all instances and set governor manager finish flag
                await self._terminate_partition_managers_and_finish()
                raise InternalError('Partition managers not started')

            _logger.info('all (%s) partition managers started successfully', len(self._partition_managers))

            # check if they registered in assumed time
            _logger.debug('waiting %s secs for partition manager registration', self._wait_for_register_timeout)

            start_of_waiting_for_registration = datetime.now()
            while len(self.managers) < len(self._partition_managers):
                await asyncio.sleep(0.2)

                if (datetime.now() - start_of_waiting_for_registration).total_seconds() >\
                        self._wait_for_register_timeout:
                    # timeout exceeded
                    _logger.error('not all partition managers registered - only %d on %d total', len(self.managers),
                                  len(self._partition_managers))
                    await self._terminate_partition_managers_and_finish()
                    raise InternalError('Partition managers not registered')

            _logger.info('available resources: %d (%d used) cores on %d nodes', self.total_resources.total_cores,
                         self.total_resources.used_cores, self.total_resources.total_nodes)

            _logger.info('all partition managers registered')
        except Exception:
            _logger.error('setup of partition managers failed: %s', str(sys.exc_info()))

    async def _terminate_partition_managers(self):
        """Terminate all partition managers"""
        try:
            await asyncio.gather(*[manager.terminate() for manager in self._partition_managers])
        except Exception:
            _logger.warning('failed to terminate manager: %s', sys.exc_info())
            _logger.error(traceback.format_exc())

    async def _terminate_partition_managers_and_finish(self):
        """Terminate all partition managers and close receiver."""
        await self._terminate_partition_managers()

        if self._receiver:
            self._receiver.set_finish(True)
        else:
            _logger.error('Failed to set finish flag due to lack of receiver access')

    def get_handler(self):
        """Return instance of handler related to the governor manager.

        As all handler methods are implemented in governor manager itself, the instance of this governor manager is
        returuned.
        """
        return self

    def set_receiver(self, receiver):
        """Set receiver related to this manager.

        Args:
            receiver (Receiver): the receiver instance
        """
        self._receiver = receiver
        if self._receiver:
            self.zmq_address = self._receiver.zmq_address

    async def stop(self):
        _logger.info(f'stopping partition managers ...')

        """Stop governor manager and cleanup all resources."""
        for manager in self.managers.values():
            await manager.finish()

        # kill all partition managers
        await self._terminate_partition_managers()

        for manager in self.managers.values():
            manager.stop()

        # stop submiting buffered tasks
        if self._schedule_buffered_jobs_task:
            try:
                self._schedule_buffered_jobs_task.cancel()
            except Exception:
                _logger.warning('failed to cancel buffered jobs scheduler task: %s', sys.exc_info()[0])

            try:
                await self._schedule_buffered_jobs_task
            except asyncio.CancelledError:
                _logger.debug('listener canceled')

    def register_notifier(self, job_state_cb, *args):
        """Register callback function for job state changes.
        The registered function will be called for all job state changes.

        Args:
            job_state_cb (def): should accept two arguments - job name and new state

        Returns:
            str: identifier of registered callback, which can be used to unregister
              callback or None if callback function is missing or is invalid
        """
        # TODO: do wee need this for governor manager ?

    async def _schedule_buffered_jobs(self):
        """Take all buffered jobs (already validated) and schedule them on available resources. """
        while not self.stop_processing:
            try:
                await asyncio.sleep(0.1)

                while self._min_scheduling_managers <= len(self.managers) and len(self._submit_reqs_buffer) > 0:
                    _logger.info('the minimum number of managers has been achieved - scheduling buffered jobs')
                    try:
                        # do not remove request from buffer before jobs will be added to the jobs database
                        # this would provide to the finish of manager (no jobs in db, no jobs in buffer)
                        req = self._submit_reqs_buffer[0]
                        await self._schedule_jobs(req)
                        # now we can safely remove request from buffer as the jobs are added to the db
                        self._submit_reqs_buffer.pop(0)
                    except Exception:
                        _logger.error('error during scheduling buffered submit requests: %s', sys.exc_info())
                        _logger.error(traceback.format_exc())
            except asyncio.CancelledError:
                _logger.info('finishing submitting buffered jobs task')
                return

    async def _wait_for_all_jobs(self):
        """Wait until all submitted jobs finish and signal receiver to finish."""
        _logger.info('waiting for all jobs to finish')

        while not self.is_all_jobs_finished():
            await asyncio.sleep(0.2)

        _logger.info('All (%s) jobs finished, no buffered jobs (%s)', len(self.jobs), len(self._submit_reqs_buffer))

        if self._receiver:
            self._receiver.set_finish(True)
        else:
            _logger.error('Failed to set finish flag due to lack of receiver access')

    def is_all_jobs_finished(self):
        """Check if all submitted jobs finished.

        Returns:
            bool: True if all submitted jobs finished
        """
        try:
            return next((job for job in self.jobs.values() if not job.status.is_finished()), None) is None \
                and len(self._submit_reqs_buffer) == 0
        except Exception:
            _logger.error('error counting jobs: %s', sys.exc_info())
            _logger.error(traceback.format_exc())
            return False

    async def handle_register_req(self, iface, request): #pylint: disable=unused-argument
        """Handle register request.

        Args:
            iface (interface): the interface from which request came
            request (RegisterReq): request

        Returns:
            Response: response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        if request.params['id'] in self.managers:
            return Response.error('Manager with id "{}" already registered')

        if not request.params['address']:
            return Response.error('Missing registry entity address')

        try:
            resources = Resources.from_dict(request.params['resources'])
            self.managers[request.params['id']] = ManagerInstance(request.params['id'],
                                                                  resources,
                                                                  request.params['address'])
            self.total_resources.append(resources)
        except Exception:
            return Response.error('Failed to register manager: {}'.format(sys.exc_info()[0]))

        _logger.info('%sth manager instance %s @ %s with resources (%s) registered successfully',
                     len(self.managers), request.params['id'], request.params['address'], request.params['resources'])

        return Response.ok(data={'id': request.params['id']})

    async def handle_control_req(self, iface, request): #pylint: disable=unused-argument
        """Handle control request.

        Control commands are used to configure system during run-time.

        Args:
            iface (interface): the interface from which request came
            request (ControlReq): control request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        if request.command != ControlReq.REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE:
            return Response.error('Not supported command "{}" of finish control request'.format(request.command))

        if self._finish_task is not None:
            return Response.error('Finish request already requested')

        self._finish_task = asyncio.ensure_future(self._wait_for_all_jobs())
        return Response.ok('{} command accepted'.format(request.command))

    async def handle_submit_req(self, iface, request): #pylint: disable=unused-argument
        """Handle submit request.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): submit request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        if self._min_scheduling_managers <= len(self.managers) and self.total_resources.total_cores == 0:
            return Response.error('Error: no resources available')

        if len(self._submit_reqs_buffer) >= self._max_buffered_jobs:
            return Response.error('Error: submit buffer overflow (currently {} buffered jobs) - try submit '
                                  'later'.format(len(self._submit_reqs_buffer)))

        try:
            # validate jobs
            self._validate_submit_req(request.jobs)
        except Exception as exc:
            _logger.error('Submit error: %s', sys.exc_info())
            _logger.error(traceback.format_exc())
            return Response.error(str(exc))

        try:
            if self._min_scheduling_managers > len(self.managers):
                # we don't have all (partition) managers registered - buffer jobs
                self._submit_reqs_buffer.append(request.jobs)
                _logger.debug('buffering submit request, current buffer size: %d', len(self._submit_reqs_buffer))

                return Response.ok('{} jobs buffered'.format(len(request.jobs)), data={'buffered': len(request.jobs)})

            # submit at once
            # split jobs equally between all available managers
            (_, job_names) = await self._schedule_jobs(request.jobs)

            data = {
                'submitted': len(job_names),
                'jobs': job_names
            }

            return Response.ok('{} jobs submitted'.format(len(job_names)), data=data)
        except Exception as exc:
            _logger.error('Submit error: %s', sys.exc_info())
            _logger.error(traceback.format_exc())
            return Response.error(str(exc))

    def _validate_submit_req(self, jobs):
        """Validate job descriptions from submit request.

        The job description is in the form of dictionaries.

        Args:
            jobs (list(dict)): the submited job descriptions

        Raises:
            JobAlreadyExist: when one of the job already exist
        """
        req_job_names = set()

        for req in jobs:
            job = req['req']

            if job['name'] in self.jobs or job['name'] in req_job_names:
                raise JobAlreadyExist('Job {} already exist'.format(job['name']))

    async def _schedule_jobs(self, jobs):
        """Schedule jobs.

        Args:
            jobs (list(dict)): job descriptions in the form of dictionary

        Returns:
            (int, list(str)): tuple with number of submitted jobs and list of submitted job names
        """
        n_jobs = 0
        job_names = []

        if self.stop_processing:
            return 0, []

        for req in jobs:
            job_desc = req['req']
            job_name = job_desc['name']

            if 'iteration' in job_desc:
                # iteration job, split between all available managers
                job_its = job_desc['iteration']
                start = job_its.get('start', 0)
                end = job_its.get('stop')
                iterations = end - start

                iter_start = start
                submit_reqs = []

                split_part = int(math.ceil(iterations / len(self.managers)))

                job_parts = []
                for idx, manager in enumerate(self.managers.values()):
                    iter_end = min(iter_start + split_part, end)

                    iter_job_desc = copy.deepcopy(job_desc)
                    iter_id = str(idx)
                    iter_job_desc.setdefault('attributes', {}).update({
                        'parent_job_id': job_name,
                        'parent_job_part_id': iter_id
                    })

                    iter_job_desc['iteration'] = {'start': iter_start, 'stop': iter_end}

                    submit_reqs.append(manager.submit_jobs([iter_job_desc]))

                    job_parts.append({'id': iter_id, 'start': iter_start, 'stop': iter_end, 'manager': manager})

                    iter_start = iter_end
                    if iter_end == end:
                        break

                submit_results = await asyncio.gather(*submit_reqs)
                for idx, result in enumerate(submit_results):
                    job_parts[idx]['local_name'] = result['names'][0]

                self._append_new_job(job_name, job_parts)

                n_jobs = n_jobs + 1
                job_names.append(job_name)
            else:
                # single job, send to the manager with lest number of submitted jobs
                manager = min(self.managers.values(), key=lambda m: m.submitted_jobs)

                _logger.debug('sending job %s to manager %s (%s)', job_desc['name'], manager.mid, manager.address)
                iter_id = "0"
                job_desc.setdefault('attributes', {}).update({
                    'parent_job_id': job_name,
                    'parent_job_part_id': iter_id
                })
                submit_result = await manager.submit_jobs([job_desc])

                _logger.debug('submit result: %s', str(submit_result))
                self._append_new_job(job_name, [{'id': iter_id,
                                                 'start': None,
                                                 'stop': None,
                                                 'local_name': submit_result['names'][0],
                                                 'manager': manager}])

                n_jobs = n_jobs + 1
                job_names.append(job_name)

        return n_jobs, job_names

    def _append_new_job(self, job_name, job_parts):
        """Add new job to the global registry.

        Args:
            job_name (str): job name
            job_parts (list(GlobalJobPart)): list of job parts
        """
        parts = []
        for job_part in job_parts:
            parts.append(GlobalJobPart(job_part['id'], job_part['local_name'], job_part['start'], job_part['stop'],
                                       JobState.QUEUED, job_part['manager']))
        self.jobs[job_name] = GlobalJob(job_name, JobState.QUEUED, parts)

    async def handle_jobstatus_req(self, iface, request): #pylint: disable=unused-argument
        """Handle job status request.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): job status request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        result = {}

        for job_name in request.job_names:
            try:
                job = self.jobs.get(job_name)

                if job is None:
                    return Response.error('Job {} doesn\'t exist'.format(request.jobName))

                result[job_name] = {'status': int(ResponseCode.OK), 'data': {
                    'jobName': job_name,
                    'status': str(job.status.name)
                }}
            except Exception as exc:
                _logger.warning('error to get job status: %s', str(exc))
                _logger.warning(traceback.format_exc())
                result[job_name] = {'status': int(ResponseCode.ERROR), 'message': exc.args[0]}

        return Response.ok(data={'jobs': result})

    async def handle_jobinfo_req(self, iface, request): #pylint: disable=unused-argument
        """Handle job info request.

        Currently not implemented.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): job info request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        # TODO: implement mechanism
        return Response.error('Currently not supported')

    async def handle_canceljob_req(self, iface, request): #pylint: disable=unused-argument
        """Handle cancel job request.

        Currently not implemented.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): cancel job request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        # TODO: implement mechanism
        return Response.error('Currently not supported')

    async def handle_removejob_req(self, iface, request): #pylint: disable=unused-argument
        """Handle remove job request.

        Currently not implemented.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): remove job request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        # TODO: implement mechanism
        return Response.error('Currently not supported')

    async def handle_listjobs_req(self, iface, request): #pylint: disable=unused-argument
        """Handle list jobs request.

        Currently not implemented.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): list jobs request data

        Returns:
            Response: the response to send back
        """
        # TODO: implement mechanism
        return Response.error('Currently not supported')

    async def handle_resourcesinfo_req(self, iface, request): #pylint: disable=unused-argument
        """Handle resources info request.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): resources info request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        return Response.ok(data={
            'total_nodes': self.total_resources.total_nodes,
            'total_cores': self.total_resources.total_cores,
            'used_cores': self.total_resources.used_cores,
            'free_cores': self.total_resources.free_cores
        })

    async def handle_finish_req(self, iface, request): #pylint: disable=unused-argument
        """Handle finish request.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): finish request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        delay = 2

        if self._finish_task is not None:
            return Response.error('Finish request already requested')

        self._finish_task = asyncio.ensure_future(self._delayed_finish(delay))

        return Response.ok(data={
            'when': '{}s'.format(delay)
        })

    async def handle_status_req(self, iface, request): #pylint: disable=unused-argument
        """Handle status request.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): status request data

        Returns:
            Response: the response to send back
        """
        if self.stop_processing:
            return Response.error('processing stopped')

        return await self.generate_status_response()

    async def handle_notify_req(self, iface, request): #pylint: disable=unused-argument
        """Handle notify request.

        Args:
            iface (interface): the interface from which request came
            request (SubmitReq): notify request data

        Returns:
            Response: the response to send back
        """
        global_job_id = request.params.get('attributes', {}).get('parent_job_id')
        global_job_part_id = request.params.get('attributes', {}).get('parent_job_part_id')

        if global_job_id is None or global_job_part_id is None:
            return Response.error('Unknown job notify data {}'.format(str(request.params)))

        job = self.jobs.get(global_job_id, None)
        if not job:
            _logger.warning('job notified %s not exist', global_job_id)
            return Response.error('Job {} unknown'.format(global_job_id))

        new_state = request.params.get('state', 'UNKNOWN')
        if new_state not in JobState.__members__:
            _logger.warning('notification for job %s contains unknown state %s', global_job_id, new_state)
            return Response.error('Job\'s {} state {} unknown'.format(global_job_id, new_state))

        if job.update_part_status(global_job_part_id, JobState[new_state]):
            _logger.debug('job state %s successfully update to %s', global_job_id, str(new_state))
            return Response.ok('job {} updated'.format(global_job_id))

        return Response.error('Failed to update job\'s {} part {} status to {}'.format(
            global_job_id, global_job_part_id, str(new_state)))

    async def _delayed_finish(self, delay):
        """Wait given number of seconds and signal receiver to finish.

        Args:
            delay (int): number of seconds to wait
        """
        _logger.info("finishing in %s seconds", delay)

        await asyncio.sleep(delay)

        if self._receiver:
            self._receiver.set_finish(True)
        else:
            _logger.error('Failed to set finish flag due to lack of receiver access')

    async def generate_status_response(self):
        """Generate current statistics about governor manager."""
        n_scheduling = n_failed = n_finished = n_executing = 0

        for job in self.jobs.values():
            if job.status in [JobState.QUEUED, JobState.SCHEDULED]:
                n_scheduling += 1
            elif job.status in [JobState.EXECUTING]:
                n_executing += 1
            elif job.status in [JobState.FAILED, JobState.OMITTED]:
                n_failed += 1
            elif job.status in [JobState.CANCELED, JobState.SUCCEED]:
                n_finished += 1

        return Response.ok(data={
            'System': {
                'Uptime': str(datetime.now() - self.start_time),
                'Zmqaddress': self._receiver.zmq_address,
                'Ifaces': [iface.name() for iface in self._receiver.interfaces]
                          if self._receiver and self._receiver.interfaces else [],
                'Host': socket.gethostname(),
                'Account': getpass.getuser(),
                'Wd': os.getcwd(),
                'PythonVersion': sys.version.replace('\n', ' '),
                'Python': sys.executable,
                'Platform': sys.platform,
            }, 'Resources': {
                'total_nodes': self.total_resources.total_nodes,
                'total_cores': self.total_resources.total_cores,
                'used_cores': self.total_resources.used_cores,
                'free_cores': self.total_resources.free_cores,
            }, 'JobStats': {
                'TotalJobs': len(self.jobs),
                'InScheduleJobs': n_scheduling,
                'FailedJobs': n_failed,
                'FinishedJobs': n_finished,
                'ExecutingJobs': n_executing,
            }})
