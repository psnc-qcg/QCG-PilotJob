import re
import json
import os
import time
import logging
import queue
from datetime import datetime
from os.path import exists, join, dirname, abspath

import multiprocessing as mp

import zmq
from qcg.pilotjob.logger import top_logger
from qcg.pilotjob.publisher import EventTopic, StatusPublisher
from qcg.pilotjob.api import errors
from qcg.pilotjob.api.jobinfo import JobInfo


top_logger = logging.getLogger('qcg.pilotjob.api')

_logger = logging.getLogger(__name__)


class TimeStamp:
    """Timestamp utility to trace timeouts and compute the poll times that do not exceed defined timeouts."""

    def __init__(self, manager, timeout_secs=None):
        """
        Create timestamp.
        During initialization the timestamp start moment is set to current time.

        :param manager (Manager): the manager instance with defined default poll and publisher timeout setting
        :param timeout (int|float): the timeout in seconds for operation related with this timestamp, the default value
          `None` means the timeout is not defined (infinity)
        """
        self.started = datetime.now()
        self.manager = manager
        self.timeout_secs = timeout_secs

    @property
    def secs_from_start(self):
        """
        Return number of seconds elapsed since start
        """
        return (datetime.now() - self.started).total_seconds()

    def check_timeout(self):
        """
        Check if timeout has been reached.
        If `timeout_secs` has been defined check if `timeout_secs` have elapsed from `started` datetime.
        If `timeout_secs` is not defined always return False


        :return: True if timeout reached, False otherwise
        """
        if self.timeout_secs is not None:
            if self.secs_from_start >= self.timeout_secs:
                raise errors.TimeoutElapsed()

    def get_poll_time(self):
        """
        Return the poll time that do not exceed timeout.
        If timeout already reached, the 0 will be returned.

        :return: poll time in seconds
        """
        if self.timeout_secs is not None:
            rest_secs = self.timeout_secs - self.secs_from_start
            if rest_secs < 0:
                raise errors.TimeoutElapsed()
        else:
            rest_secs = self.manager.default_poll_delay

        _logger.info(f'poll time set to: min({self.manager.default_poll_delay}, {rest_secs})')
        return min(self.manager.default_poll_delay, rest_secs)

    def get_events_timeout(self):
        """
        Return the subscribe timeout that do not exceed total operation timeout.
        If timeout already reached, the 0 will be returned.

        :return: the timeout time in seconds
        """
        if self.timeout_secs is not None:
            rest_secs = self.timeout_secs - self.secs_from_start
            if rest_secs < 0:
                raise errors.TimeoutElapsed()
        else:
            rest_secs = self.manager.default_pub_timeout

        _logger.info(f'events timeout set to: min({self.manager.default_pub_timeout}, {rest_secs})')
        return min(self.manager.default_pub_timeout, rest_secs)


class Manager:
    """The Manager class is used to communicate with single QCG-PilotJob manager instance.

    We assume that QCG-PilotJob manager instance is already running with ZMQ interface. The communication with
    QCG-PilotJob is fully synchronous.
    """

    DEFAULT_ADDRESS_ENV = "QCG_PM_ZMQ_ADDRESS"
    DEFAULT_ADDRESS = "tcp://127.0.0.1:5555"
    DEFAULT_PROTO = "tcp"
    DEFAULT_PORT = "5555"

#    DEFAULT_POLL_DELAY = 30
    DEFAULT_POLL_DELAY = 5
    DEFAULT_PUB_TIMEOUT = 5 * 60


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
              'default_poll_delay' - the default delay between following status polls in wait methods
              'default_pub_timeout' - the default timeout for waiting on published events
              'log_file' - the location of the log file
              'log_level' - the log level ('DEBUG'); by default the log level is set to INFO
        """
        self._zmq_ctx = zmq.Context()
        self._zmq_socket = None
        self._zmq_status_socket = None
        self._zmq_status_poller = None
        self._connected = False

        self._publisher_address = None
        self.default_poll_delay = Manager.DEFAULT_POLL_DELAY
        self.default_pub_timeout = Manager.DEFAULT_PUB_TIMEOUT

        # as the `_setup_logging` method might be called from the `LocalManager` class before this class `__init__`
        # the `log_handler` field might be already initialized
        if getattr(self, 'log_handler', None) is None:
            self.log_handler = None

        client_cfg = cfg or {}
        self._setup_logging(client_cfg)

        if address is None:
            if Manager.DEFAULT_ADDRESS_ENV in os.environ:
                address = os.environ[Manager.DEFAULT_ADDRESS_ENV]
                _logger.debug('found zmq address of pm: %s', address)
            else:
                address = Manager.DEFAULT_ADDRESS

        if 'poll_delay' in client_cfg:
            self.default_poll_delay = client_cfg['poll_delay']

        if 'pub_timeout' in client_cfg:
            self.default_pub_timeout = client_cfg['pub_timeout']

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
        if getattr(self, 'log_handler', None) is None:
            wdir = cfg.get('wdir', '.')
            _log_file = cfg.get('log_file', join(wdir, '.qcgpjm-client', 'api.log'))

            if not exists(dirname(abspath(_log_file))):
                os.makedirs(dirname(abspath(_log_file)))
            elif exists(_log_file):
                os.remove(_log_file)

            self.log_handler = logging.FileHandler(filename=_log_file, mode='a', delay=False)
            self.log_handler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
            top_logger.addHandler(self.log_handler)

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
            self._zmq_socket = self._zmq_ctx.socket(zmq.REQ) # pylint: disable=maybe-no-member
            self._zmq_socket.connect(self._address)
            self._connected = True
            _logger.info("connection created")

            _logger.info("checking system status")
            system_status = self.system_status().get('System', {})

            _logger.info(f"successfully connected to the instance {system_status.get('InstanceId')} @ "
                         f"{system_status.get('Host')}")

            self._publisher_address = system_status.get('StatusPublisher')
            if self._publisher_address:
                self._zmq_status_socket = self._zmq_ctx.socket(zmq.SUB) # pylint: disable=maybe-no-member
                self._zmq_status_socket.connect(self._publisher_address)

                _logger.info(f"created subscription socket for job status changed @ {self._publisher_address}")
                self._zmq_status_socket.setsockopt_string(zmq.SUBSCRIBE, EventTopic.ITERATION_FINISHED.value) # pylint: disable=maybe-no-member
                self._zmq_status_socket.setsockopt_string(zmq.SUBSCRIBE, EventTopic.JOB_FINISHED.value) # pylint: disable=maybe-no-member
                self._zmq_status_socket.setsockopt_string(zmq.SUBSCRIBE, EventTopic.NO_JOBS.value) # pylint: disable=maybe-no-member

                self._zmq_status_poller = zmq.Poller()
                self._zmq_status_poller.register(self._zmq_status_socket, zmq.POLLIN) # pylint: disable=maybe-no-member

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
        if getattr(self, 'log_handler', None):
            self.log_handler.close()
            top_logger.removeHandler(self.log_handler)

    def system_status(self):
        return self._send_and_validate_result({
            "request": "status"
        })


    def wait4(self, names, timeout=None):
        """Wait for finish of specific jobs.

        This method waits until all specified jobs finish its execution (successfully or not).
        The QCG-PilotJob manager is periodically polled about status of not finished jobs. The poll interval (2 sec by
        default) can be changed by defining a 'poll_delay' key with appropriate value (in seconds) in
        configuration of instance.

        Args:
            names (str|list(str)): list of job names to get detailed information about
            timeout (int|float): maximum number of seconds to wait

        Returns:
            dict - a map with job names and their terminal status

        Raises:
            TimeoutElapsed: in case of timeout elapsed
            InternalError: in case the response format is invalid
            ConnectionError: in case of non zero exit code, or if connection has not been established yet
        """
        if isinstance(names, str):
            job_names = [names]
        else:
            job_names = list(names)

        _logger.info("waiting for finish of %d jobs", len(job_names))

        result = {}
        not_finished = set(job_names)

        started_ts = TimeStamp(self, timeout)

        while len(not_finished) > 0:
            started_ts.check_timeout()

            try:
                jobs_status = self.status(list(not_finished))

                for job_name, job_data in jobs_status['jobs'].items():
                    if 'status' not in job_data['data'] or job_data['status'] != 0 or 'data' not in job_data:
                        raise errors.InternalError(f"Missing job's {job_name} data")

                    if Manager.is_status_finished(job_data['data']['status']):
                        not_finished.remove(job_name)
                        result[job_name] = job_data['data']['status']

                if len(not_finished) > 0:
                    _logger.info(f"still {len(not_finished)} jobs not finished")

                    if self._zmq_status_poller:
                        while len(not_finished):
                            started_ts.check_timeout()

                            _logger.info('waiting for new events ...')
                            events = self._zmq_status_poller.poll(started_ts.get_events_timeout() * 1000)
                            if events:
                                for _ in events:
                                    try:
                                        status_event = self._zmq_status_socket.recv_string()
                                        _logger.info(f'got status event {status_event}')
                                        topic, event_data = StatusPublisher.decode_published_data(status_event)
                                        if topic in [EventTopic.JOB_FINISHED, EventTopic.ITERATION_FINISHED]:
                                            if event_data.get('it') is not None:
                                                job_name = f'{event_data.get("job")}:{event_data.get("it")}'
                                            else:
                                                job_name = event_data.get('job')

                                            if job_name in not_finished:
                                                not_finished.remove(job_name)
                                                result[job_name] = event_data.get("state")
                                    except Exception:
                                        _logger.exception('error during processing status event')
                    else:
                        time.sleep(started_ts.get_poll_time())
            except errors.TimeoutElapsed:
                raise
            except Exception as exc:
                raise errors.ConnectionError(exc.args[0])

        _logger.info("all jobs finished")
        return result


    def wait4all(self, timeout_secs=None):
        """Wait for finish of all submitted jobs.

        :arg timeout_secs (int|float): optional timeout setting in seconds

        :raise TimeoutElapsed when timeout elapsed (if defined as argument)

        This method waits until all jobs submitted to service finish its execution (successfully or not).
        """
        started_ts = TimeStamp(self, timeout_secs)

        status = self._send_and_validate_result({
            "request": "status",
            "options": { "allJobsFinished": True }
        })
        all_finished = status.get("AllJobsFinished", False)

        if not self._zmq_status_poller:
            while not all_finished:
                time.sleep(started_ts.get_poll_time())
                started_ts.check_timeout()
                status = self._send_and_validate_result({
                    "request": "status",
                    "options": { "allJobsFinished": True }
                })
                all_finished = status.get("AllJobsFinished", False)

        else:
            events = self._zmq_status_poller.poll(0)
            if events:
                all_finished = False

            while not all_finished:
                for e in events:
                    try:
                        status_event = self._zmq_status_socket.recv_string()
                        _logger.info(f'got status event {status_event}')
                        topic, _ = StatusPublisher.decode_published_data(status_event)
                        if topic == EventTopic.NO_JOBS:
                            # we've got NO_JOBS event
                            all_finished = True
                            break
                    except Exception:
                        _logger.exception('error during processing status event')

                if not all_finished:
                    _logger.info('waiting for new events ...')
                    events = self._zmq_status_poller.poll(started_ts.get_events_timeout() * 1000)
                    started_ts.check_timeout()

        _logger.info("all jobs finished in manager")

    def wait4_any_job_finish(self, timeout_secs=None):
        """Wait for finish one of any submitted job.

        This method waits until one of any jobs submitted to service finish its execution (successfully or not).

        :arg timeout_secs (float|int) - timeout in milliseconds, endlessly (None) by default

        :raise TimeoutElapsed when timeout elapsed (if defined as argument)

        :return (str, str) identifier of finished job and it's status or None, None if timeout has been reached.
        """
        if not self._zmq_status_poller:
            raise errors.ServiceError('Service not support publishing status events')

        started_ts = TimeStamp(self, timeout_secs)

        while True:
            started_ts.check_timeout()

            events = self._zmq_status_poller.poll(started_ts.get_events_timeout() * 1000)
            if events:
                try:
                    status_event = self._zmq_status_socket.recv_string()
                    _logger.info(f'got status event {status_event}')
                    topic, data = StatusPublisher.decode_published_data(status_event)
                    if topic in [EventTopic.JOB_FINISHED, EventTopic.ITERATION_FINISHED]:
                        if data.get('it') is not None:
                            jobid = f'{data.get("job")}:{data.get("it")}'
                        else:
                            jobid = data.get('job')

                        return jobid, data.get('state')
                except Exception as exc:
                    _logger.exception('Error in event processing')
                    raise errors.ServiceError(f'Error in event processing: {str(exc)}')

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
              'init_timeout' - the timeout (in seconds) client should wait for QCG-PilotJob manager start until it raise
                 error, 300 by default
              'poll_delay' - the delay between following status polls in wait methods
              'log_file' - the location of the log file
              'log_level' - the log level ('DEBUG'); by default the log level is set to INFO
        """
        client_cfg = cfg or {}
        self._setup_logging(client_cfg)

        _logger.debug('initializing MP start method with "fork"')
        mp.set_start_method("fork", force=True)
        mp.freeze_support()

        if LocalManager.is_notebook():
            _logger.debug('Creating a new event loop due to run in an interactive environment')
            import asyncio
            asyncio.set_event_loop(asyncio.new_event_loop())
                        
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

        try:
            # timeout of single iteration
            wait_single_timeout = 2

            # number of seconds
            wait_seconds = int(client_cfg.get('init_timeout', 300))

            service_wait_start = datetime.now()
            _logger.info(f'{service_wait_start} waiting {wait_seconds} secs for service start ...')

            while (datetime.now() - service_wait_start).total_seconds() < wait_seconds:
                if not self.qcgpm_process.is_alive():
                    raise errors.ServiceError('Service process killed')

                try:
                    self.qcgpm_conf = self.qcgpm_queue.get(block=True, timeout=wait_single_timeout)
                    break
                except queue.Empty:
                    continue
                except Exception as exc:
                    raise errors.ServiceError('Service not started: {}'.format(str(exc)))

            if not self.qcgpm_conf:
                raise errors.ServiceError(f'Service not started in {(datetime.now() - service_wait_start).total_seconds():.1f}')

            if self.qcgpm_conf.get('error', None):
                raise errors.ServiceError(self.qcgpm_conf['error'])
        except Exception as ex:
            if self.qcgpm_process:
                try:
                    _logger.debug('killing pilotjob service process as not started properly')
                    self.qcgpm_process.terminate()
                except:
                    _logger.exception('failed to kill pilotjob service')
            raise

        _logger.info(f'service started after {(datetime.now() - service_wait_start).total_seconds()} secs')

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

    @staticmethod
    def is_notebook():
        try:
            shell = get_ipython().__class__.__name__
            if shell == 'ZMQInteractiveShell':
                return True  # Jupyter notebook or qtconsole
            else:
                return False  # Other type (?)
        except NameError:
            return False  # Probably standard Python interpreter

