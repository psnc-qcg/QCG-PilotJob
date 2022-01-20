import asyncio
import logging


class RequestHandler:
    """Base class for request handlers.
    All child classes should define two attributes:
        - requests (list(str)): list of request names they are supporting
        - handler_prefix (str): prefix for handler methods
    All supported request handler should define a method with name as:
        {handler_prefix}{request_name}
    """

    def is_supported(self, request_name):
        return request_name in getattr(self, 'requests')

    def get_handler(self, request_name):
        return getattr(self, f'{getattr(self, "handler_prefix")}{request_name}')


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
            self.listen_task = asyncio.ensure_future(self.listen())
        except:
            logging.exception('Failed to start listener')
            raise

    async def stop(self):
        """Stop listening on interface."""

        await self.cancel()

        if self.iface:
            try:
                self.iface.stop()
            except:
                logging.exception('failed to close interface')

            self.iface = None

        logging.info('receiver stopped')

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

        if not self.handler.is_supported(request.get('cmd', '').lower()):
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
                    break

                logging.debug(f'interface {self.iface.external_address} new request #{req_nr}: {str(request)}')
                not_valid_msg = self.is_valid_request(request)
                if not_valid_msg:
                    response = {'error': not_valid_msg}
                else:
                    try:
                        response = await self.handle_request(request)
                    except Exception as exc:
                        logging.exception('handler error')
                        response = {'error': str(exc)}

                logging.debug(f'interface {self.iface.external_address} sending #{req_nr} request response: '
                              f'{str(response)}')

                await self.iface.reply(response)
            except asyncio.CancelledError:
                logging.info(f'finishing listening on interface {self.iface.external_address}')
                break
            except:
                logging.exception(f'error during listening on interface {self.iface.external_address}')

        logging.info('receiver listener finished')

    async def handle_request(self, request):
        return self.handler.get_handler(request.get('cmd', '').lower())(request).to_dict()