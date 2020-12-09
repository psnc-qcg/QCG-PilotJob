import zmq
import re
import socket
import asyncio
import logging

from zmq.asyncio import Context


class ZMQInterface:

    def __init__(self):
        """ZMQ interface for QCG-PilotJob InputQueue serivce."""

        self.zmq_ctx = None
        self.socket = None
        self.address = None
        self.external_address = None

    def start(self, config):
        """Setup interface. """
        self.zmq_ctx = Context.instance()
        self.address = config.get(Config.ZMQ_IFACE_ADDRESS)
        self.socket = self.zmq_ctx.socket(zmq.REP) #pylint: disable=maybe-no-member

        if re.search(r':[0-9]+$', self.address):
            self.socket.bind(self.address)
        else:
            self.socket.bind_to_random_port(self.address,
                                            min_port=int(config.get(Config.ZMQ_PORT_MIN_RANGE)),
                                            max_port=int(config.get(Config.ZMQ_PORT_MAX_RANGE)))

        real_address = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT))) #pylint: disable=maybe-no-member
        self.external_address = self.real_address
        if '//0.0.0.0:' in self.real_address:
            self.external_address = self.real_address.replace('//0.0.0.0:',
                                                              f'//{socket.gethostbyname(socket.gethostname())}:')

    async def receive(self):
        """Wait for incoming request.
        Each receive must be followed by reply.

        Returns:
            obj: incoming message as json object
        """
        return await self.socket.recv_json()

    async def reply(self, message):
        """Send reply message.
        The reply method should be called on every received message.

        Arguments:
            message (obj): a json object to send
        """
        await self.socket.send_json(message)


class Receiver:

    def __init__(self):
        """The input interface for QCG-PilotJob InputQueue service."""
        self.iface = None
        self.listen_task = None
        self.handler = None

    def start(self, handler, iface):
        """Start interface asynchronous listener.

        Arguments:
            handler (object): a handler object that defines `requests` and `handle` functions
                - the `is_supported` function should true if request with given name is supported
                - the `get_handler` function should return asynchronous handler function for given request name
            iface (Interface): interface to use
        """
        self.iface = iface
        self.handler = handler

        try:
            self.listen_task = asyncio.ensure_future(self._listen)
        except:
            logging.exception('Failed to start listener')

    async def stop(self):
        """Stop listening on interface."""

        self.cancel()

        if self.iface:
            try:
                self.iface.close()
            except:
                logging.exception('failed to close interface')

            self.iface = None

    async def cancel(self):
        """Stop listener task.
        To clean finish, the input interface should be also called. In 99% cases, the `stop` method should be called
        instead of cancel.
        """
        if self.listen_task:
            logging.info('canceling interface listener')

            try:
                self.listen_task.cancel()
            except Exception as exc:
                logging.warning(f'failed to cancel interface listener: {str(exc)}')

            try:
                await self.listen_task
            except asyncio.CancelledError:
                logging.debug('interface listener canceled')

            self.listen_task = None

    def is_valid_request(self, request):
        """Check if incoming request is valid.

        Arguments:
            request (obj) - received request

        Returns:
            str: an error message if request is not valid, None otherwise
        """
        if 'cmd' not in request:
            return 'missing request command (`cmd` element)'

        if not self.handler.is_supported(request.get('cmd', '').lowercase()):
            return f'unknown request `{request.get("cmd")}'

        return None

    async def listen(self):
        logging.info(f'starting listening on interface {self.iface.external_address}')

        req_nr = 0

        while True:
            try:
                request = await self.iface.receive()
                if request is None:
                    logging.info(f'finishing listening on interface {self.iface.external_address} - no more data')

                logging.debug(f'interface {self.iface.external_address} new request #{req_nr}: {str(request)}')
                not_valid_msg = self.is_valid_request(request)
                if not_valid_msg:
                    response = {'error': not_valid_msg}
                else:
                    try:
                        response = await self.handle_request(request)
                    except Exception as exc:
                        response = {'error': str(exc)}

                logging.debug(f'interface {self.iface.external_address} sending #{req_nr} request response: '
                              f'{str(response)}')

                await self.iface.reply(response)
            except asyncio.CancelledError:
                logging.info(f'finishing listening on interface {self.iface.external_address}')
            except:
                logging.exception(f'error during listening on interface {self.iface.external_address}')

    async def handle_request(self, request):
        return await self.handler.get_handler(request.get('cmd', '').lower)(request)

