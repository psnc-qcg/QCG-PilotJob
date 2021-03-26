import asyncio
import logging
import sys
from asyncio import CancelledError
from enum import Enum

from qcg.pilotjob.errors import InvalidRequest
from qcg.pilotjob.request import RegisterReq, ListJobsReq, ResourcesInfoReq, FinishReq
from qcg.pilotjob.request import RemoveJobReq, ControlReq, StatusReq, NotifyReq
from qcg.pilotjob.request import Request, SubmitReq, JobStatusReq, JobInfoReq, CancelJobReq
from qcg.pilotjob.response import Response
from qcg.pilotjob.zmqinterface import ZMQInterface


_logger = logging.getLogger(__name__)


class ResponseStatus(Enum):
    """Request response status."""

    UNKNOWN = 1
    ERROR = 2
    SUCCESS = 3


class ValidateResponse:
    """The response data.

    Attributes:
        result (ResponseStatus): status
        msg (str): response message
        request (Request): request
    """

    def __init__(self):
        """Initialize response."""
        self.result = ResponseStatus.UNKNOWN
        self.msg = None
        self.request = None

    def error(self, err_msg):
        """Set response as an error.

        Args:
            err_msg (str): response error message
        """
        self.result = ResponseStatus.ERROR
        self.request = None
        self.msg = err_msg

    def success(self, request):
        """Set response as a success.

        Args:
            request (Request): the original request
        """
        self.result = ResponseStatus.SUCCESS
        self.request = request
        self.msg = 'Ok'

    @property
    def is_error(self):
        """bool: is an error response"""
        return self.result == ResponseStatus.ERROR

    @property
    def is_success(self):
        """bool: is a success response"""
        return self.result == ResponseStatus.SUCCESS


class Receiver:
    """The receiver listens for requests on input interfaces and passes requests to handlers (managers).

    Attributes:
        _ifaces (list(Interface)): list of interfaces to listen for requests
        _tasks (list(asyncio.Future)): list of tasks that listens on interfaces for incoming requests
        _zmq_address (str): address of the ZMQ input interface
        _handler (Manager): object which handles incoming requests
        _handlers (dict): map of requests and handler functions
        finished (bool): flag set by the handlers when receiving should be finished
    """

    def __init__(self, handler, ifaces):
        """Initialize receiver.

        Args:
            handler: manager that handles requests
            ifaces (Interface[]): list of interfaces
        """
#        assert ifaces is not None and isinstance(ifaces, list) and len(ifaces) > 0
        assert ifaces is not None

        self._handler = handler

        self._active_ifaces = 0
        self._inited_ifaces = 0

        self._ifaces = ifaces
        self._tasks = []

        self._zmq_address = self._find_zmq_address()

        self._handlers = {
            RegisterReq: self._handler.handle_register_req,
            ControlReq: self._handler.handle_control_req,
            SubmitReq: self._handler.handle_submit_req,
            JobStatusReq: self._handler.handle_jobstatus_req,
            JobInfoReq: self._handler.handle_jobinfo_req,
            CancelJobReq: self._handler.handle_canceljob_req,
            RemoveJobReq: self._handler.handle_removejob_req,
            ListJobsReq: self._handler.handle_listjobs_req,
            ResourcesInfoReq: self._handler.handle_resourcesinfo_req,
            FinishReq: self._handler.handle_finish_req,
            StatusReq: self._handler.handle_status_req,
            NotifyReq: self._handler.handle_notify_req,
        }

        self.finished = False
        self._handler.set_receiver(self)

        # in case where there are no interfaces, set ``finished ``flag to True
        # to not block the finishing of QCG-PilotJob
        if len(self._ifaces) == 0:
            self.finished = True


    def _find_zmq_address(self):
        """Look for ZMQ interface and get input address of this interface.

        Returns:
            str: input address of first ZMQ interface, or None if no such interface is available
        """
        zmq_iface = next((iface for iface in self._ifaces if isinstance(iface, ZMQInterface)), None)
        if zmq_iface:
            return zmq_iface.external_address

        return None

    @property
    def zmq_address(self):
        """str: address of ZMQ interface"""
        return self._zmq_address

    @property
    def interfaces(self):
        """list(Interface): list of input interfaces"""
        return self._ifaces

    def set_finish(self, finished):
        """Set finish flag.

        If set to True the receiver should not accept any new requests and whole service should finish.
        The service will monitor this flag to know when receiver finished accepting requests.

        Args:
            finished (bool): the finish flag
        """
        self.finished = finished

    @property
    def is_finished(self):
        """bool: the value of finish flag"""
        return self.finished

    def _started_iface(self, iface):
        self._active_ifaces = self._active_ifaces + 1
        self._inited_ifaces = self._inited_ifaces - 1
        _logger.info(f'receiver - interface {iface.__class__.__name__} activated ({self._active_ifaces} active)')

    def _stopped_iface(self, iface):
        self._active_ifaces = self._active_ifaces - 1
        _logger.info(f'receiver - interface {iface.__class__.__name__} stopped ({self._active_ifaces} active, {self._inited_ifaces} initialized)')

        if self._inited_ifaces == 0 and self._active_ifaces == 0:
            _logger.info('No more active interfaces - finishing')
            self.set_finish(True)

    async def _listen(self, iface):
        """Task that listen on given interface and handles the incoming requests.

        Args:
            iface (Interface): interface to listen to
        """
        self._started_iface(iface)
        _logger.info(f'Listener on interface {iface.__class__.__name__} started')

        while True:
            try:
                request = await iface.receive()

                if request is None:
                    # finishing listening - nothing more will come
                    _logger.info(f'Finishing listening on interface {iface.__class__.__name__} due to EOD')

                    self._stopped_iface(iface)
                    return

                _logger.info('Interface %s received request: %s', iface.__class__.__name__, str(request))

                # validate request
                valid_response = Receiver._validate(request)

                if valid_response.is_error:
                    _logger.debug('validate request failed: result(%s), msg(%s), request(%s)',
                                  str(valid_response.result), str(valid_response.msg), str(valid_response.request))
                    response = Response.error(valid_response.msg)
                else:
                    response = await self._handle_request(iface, valid_response.request)

                _logger.info('sending response: %s', str(response.to_dict()))
                await iface.reply(response.to_json())
            except CancelledError:
                # listener was canceled - finished gracefully
                _logger.info(f'Finishing listening on interface {iface.__class__.__name__} due to interrupt')
                self._stopped_iface(iface)
                return
            except Exception:
                _logger.exception(f'Failed to process request from interface {iface.__class__.__name__}')
                self._stopped_iface(iface)

    async def _handle_request(self, iface, request):
        """Handle single request.

        The proper handler for given request type should be found (in handlers map) and
        called.

        Args:
            iface (Interface): interface which received request
            request (Request): parsed request data

        Returns:
            Response: response that should be returned to user
        """
        if request.__class__ not in self._handlers:
            _logger.error('Failed to handle request: unknown request class "%s"', request.__class__.__name__)
            return Response.error('Unknown request type "{}"'.format(request.__class__.__name__))

        try:
            _logger.info('Handling %s request from %s interface',
                         request.__class__.__name__, iface.__class__.__name__)

            return await self._handlers[request.__class__](iface, request)
        except Exception:
            _logger.exception('Failed to process request: %s', sys.exc_info()[0])
            return Response.error('Failed to process request: {}'.format(sys.exc_info()[0]))

    @staticmethod
    def _validate(request):
        """Validate incoming request and parse it.

        In this first step only form of JSON format is validated along with some basic checks.
        More specific validation should be provided on further steps.

        Args:
            request (dict): request data

        Returns:
            ValidateResponse: validation result
        """
        response = ValidateResponse()
        req = None

        try:
            req = Request.parse(request)
            response.success(req)
        except InvalidRequest as exc:
            _logger.error("Failed to parse request - invalid request")
            response.error('Invalid request: {}'.format(exc.args[0]))
        except Exception:
            _logger.exception('Failed to parse request: %s', sys.exc_info()[0])
            response.error('Invalid request: {}'.format(sys.exc_info()[0]))

        # request is OK, but we have to validate request parameters, such as:
        # cancelJob, statusJob job names exists or submitJob job name uniqness
        return response

    def run(self):
        """Start listening on interfaces.

        This method creates asynchronic tasks and returns. To stop created tasks, method 'stop'
        must be called.
        """
        # are old tasks should be stoped here ?
        self._tasks = []

        for iface in self._ifaces:
            try:
                _logger.info('Starting listener on interface %s', iface.__class__.__name__)
                task = asyncio.ensure_future(self._listen(iface))
            except Exception:
                _logger.exception('Failed to start listener for %s interface', iface.__class__.__name__)
                task = None

            if task is not None:
                self._tasks.append(task)

        self._inited_ifaces = len(self._tasks)
        _logger.info('Successfully initialized %d interfaces', len(self._tasks))

    async def stop(self):
        """Stop all listening on interfaces."""
        await self.cancel_listeners()

        for iface in self._ifaces:
            try:
                iface.close()
            except Exception:
                _logger.warning('failed to close interface: %s', str(sys.exc_info()))

    async def cancel_listeners(self):
        """Cancel all interface listeners."""
        _logger.info('canceling %d listener tasks', len(self._tasks))

        for task in self._tasks:
            if task is not None:
                _logger.info('canceling listener task')
                try:
                    task.cancel()
                except Exception:
                    _logger.warning('failed to cancel listener task: %s', sys.exc_info()[0])

                try:
                    await task
                except asyncio.CancelledError:
                    _logger.debug('listener canceled')

        self._tasks = []

    def generate_status_response(self):
        """Get current statistics from handler/manager.

        Returns:
            Response: response with current statistics as a data
        """
        return self._handler.generate_status_response()
