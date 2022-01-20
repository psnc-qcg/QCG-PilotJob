import logging
import asyncio
import zmq
from zmq.asyncio import Context
from qcg.pilotjob.common.eventmonitor import Event

_logger = logging.getLogger(__name__)


class Request:
    # default timeout for receive (response) for this request
    DEFAULT_RECV_TIMEOUT = 5

    def __init__(self, data, ignore_result=False, with_event=False, timeout=None):
        """Request to be sent to the Input Queue instance.

        Args:
            data (object): json serializable data to be sent as message to the input queue instance
            ignore_result (bool): true if result response should not be stored
            with_event (bool): true if an result event should be created and waiting for the result is allowed
        """
        self.data = data
        self.result = None
        self.event = None
        self.timeout = timeout or Request.DEFAULT_RECV_TIMEOUT

        self.ignore_result = ignore_result

        if with_event:
            self.event = asyncio.Event()

    def signal_result(self, result):
        """Set request response.
        If result event has been created, after seting the results the event is activated.

        Args:
            result (object): request response
        """
        print(f'got request response: {result}')
        if not self.ignore_result:
            self.result = result

        if self.event:
            self.event.set()

    async def wait_for_result(self):
        """Wait for the response and return response result.
        The `with_event` argument to the `Request` must be set.

        Returns:
            object: the response message.
        """
        if not self.event:
            raise ValueError('request created without result event signal')

        await self.event.wait()
        return self.result


class RequestSender:

    def __init__(self, address, event_publisher=None, send_event_type=None, recv_event_type=None):
        """Send queued requests to the remote address with PAIR model of zmq communication patterns.
        The instance should be started with `start` method. The requests to be sent should be enqueued
        with `send` method.

        Args:
            address (str): remote address to send requests
        """
        self.address = address
        self.zmq_ctx = Context.instance()

        self.out_socket = None
        self.out_requests = asyncio.Queue()
        self.out_socket_poller = None

        self.sender_task = None

        if event_publisher and any((send_event_type is None, recv_event_type is None)):
            raise ValueError('missing send or/any recv event type')

        self.event_publisher = event_publisher
        self.send_event_type = send_event_type
        self.recv_event_type = recv_event_type

    def start(self):
        """Connect to the remote address and start sending enqueued requests."""
        self._connect()
        self.sender_task = asyncio.ensure_future(self._request_sender())

    def send(self, request):
        """Enqueue request to be sent.
        In case where the queue is full, the `asyncio.QueueFull` exception will be raised.

        Args:
            request (Request): request to be send
        """
        self.out_requests.put_nowait(request)

    async def stop(self):
        """Stop sending requests and close connection to the remote address."""
        if not self.sender_task:
            _logger.warning('sender not started')
            return

        self.sender_task.cancel()
        try:
            await asyncio.wait_for(self.sender_task, 5)
        except asyncio.TimeoutError:
            _logger.error('failed to stop sender')
        except asyncio.CancelledError:
            pass
        finally:
            self.sender_task = None
            self._disconnect()
        _logger.info('request sender stopped')

    def _connect(self):
        """Create an output socket and connect to the remote address.
        WARNING: in case where address is wrong or communication link is incorrect, the error occur while
        sending the first message.
        """
        self.out_socket = self.zmq_ctx.socket(zmq.REQ) # pylint: disable=maybe-no-member

        if not self.out_socket_poller:
            self.out_socket_poller = zmq.Poller()

        self.out_socket_poller.register(self.out_socket, zmq.POLLIN) # pylint: disable=maybe-no-member
        self.out_socket.connect(self.address)

    def _disconnect(self):
        """Disconnect from remote address and close socket."""
        if self.out_socket:
            self.out_socket.disconnect(self.address)
            self.out_socket.close()

            if self.out_socket_poller:
                self.out_socket_poller.unregister(self.out_socket)

            self.out_socket = None

    async def _request_sender(self):
        """Process requests to be sent to the input queue.
        The requests enqueued in the `out_requests` are read and sent to the input queue.
        """
        _logger.info('request sender initialized')

        if not self.out_socket:
            _logger.error('socket not initialized')
            raise IOError('output socket not initialized')

        try:
            while True:
                request = await self.out_requests.get()
                try:
                    if self.event_publisher:
                        self.event_publisher.publish(Event(self.send_event_type, {'request': request.data}))

                    await self.out_socket.send_json(request.data)
                    print(f'json {request.data} send')

                    events = self.out_socket_poller.poll(request.timeout * 1000)
                    result = None
                    if events:
                        for _ in events:
                            try:
                                result = await self.out_socket.recv_json()
                                if self.event_publisher:
                                    self.event_publisher.publish(Event(self.recv_event_type, {'result': result}))
                                request.signal_result(result)
                            except Exception:
                                _logger.exception("error during processing response")
                    else:
                        # signal receive timeout
                        if self.event_publisher:
                            self.event_publisher.publish(Event(self.recv_event_type, {'result': result}))
                        request.signal_result(None)
                finally:
                    self.out_requests.task_done()

        except asyncio.CancelledError:
            raise
        finally:
            _logger.info('finishing sender task')

