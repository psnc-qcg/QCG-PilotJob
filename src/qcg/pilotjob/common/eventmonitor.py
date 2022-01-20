from enum import Enum
import asyncio
import logging
import zmq
import socket
from zmq.asyncio import Context

from datetime import datetime


class EventType(Enum):

    ALL = 0
    NEW_JOB = 1


class Event:

    def __init__(self, event_type, data, tags=None):
        """Event data.

        time (datetime) - event occurrence time

        Args:
            event_type (EventType) - event type
            data (Object) - data related and specific for event
            tags (dict) - some dictionary describing specific event
        """
        self.time = datetime.now()
        self.event_type = event_type
        self.data = data
        self.tags = tags or {}


class EventListener:

    def __init__(self, caller, tags, *args, **kwargs):
        """Listener registered for events.

        Args:
            caller (callable) - listener function
            tags (dictionary) - dictionary for filtering events
            args (list) - list of arguments
            kwargs (dict) - list of named arguments
        """
        if not callable(caller):
            raise TypeError(f'Object {caller!r} must is not callable')

        self.caller = caller
        self.tags = tags or {}
        self.args = args
        self.kwargs = kwargs

    def match_tags(self, event_tags):
        """Check if listener tags matches event tags.

        Currently the match is True when:
            * all listener tags exists in event tags
            * all listener tag values matches event tags values

        Args:
            event_tags (dict) - event tags
        """
        logging.debug(f'checking listener tags {self.tags} with event tags {event_tags}')
        e_tags = event_tags or {}
        for tag_name, tag_value in self.tags.items():
            if tag_name not in e_tags or e_tags.get(tag_name) != tag_value:
                return False

        return True

    def call(self, event):
        """Call listener with given event.

        Args:
            event (Event) - the event to call with listener
        """
        self.caller(event, *self.args, **self.kwargs)





class EventPublisher:

    def __init__(self, with_remote_iface=False):
        """The event publishing/listening manager."""
        self.listeners = {}
        self.event_queue = asyncio.Queue()
        self.queue_handler = None

        self.remote_iface_task = None
        self.remote_iface_address = None
        self.remote_iface_socket = None
        if with_remote_iface:
            self.setup_remote_iface()

        self.start()

    def start(self):
        """Start event publishing handler."""
        self.queue_handler = asyncio.ensure_future(self._queue_handler())

    def setup_remote_iface(self):
        zmq_ctx = Context.instance()
        self.remote_iface_socket = zmq_ctx.socket(zmq.REP) #pylint: disable=maybe-no-member
        self.remote_iface_socket.bind_to_random_port('tcp://*', min_port=2222, max_port=9999)
        real_address = str(bytes.decode(self.remote_iface_socket.getsockopt(zmq.LAST_ENDPOINT))) #pylint: disable=maybe-no-member
        self.remote_iface_address = real_address
        if '//0.0.0.0:' in real_address:
            self.remote_iface_address = self.remote_iface_address.replace(
                '//0.0.0.0:', f'//{socket.gethostbyname(socket.gethostname())}:')

        self.remote_iface_task = asyncio.ensure_future(self._remote_iface_handler())
        logging.info(f'event publisher remote interface address {self.remote_iface_address}')

    async def stop(self):
        """Stop event publishing handler."""
        logging.debug('stoping event handler')
        if not self.queue_handler:
            logging.warning('event handler not started')
            return

        self.queue_handler.cancel()
        try:
            await asyncio.wait_for(self.queue_handler, 5)
        except asyncio.TimeoutError:
            logging.error('failed to stop event handler')
        except asyncio.CancelledError:
            pass
        finally:
            self.queue_handler = None
        logging.info('event handler stopped')

        if self.remote_iface_task:
            logging.debug('stopping remote event listener')

            self.remote_iface_task.cancel()
            try:
                await asyncio.wait_for(self.remote_iface_task, 5)
            except asyncio.TimeoutError:
                logging.error('failed to stop remote event listener')
            except asyncio.CancelledError:
                pass
            finally:
                self.remote_iface_task = None
            logging.info('remote event listener stopped')

        if self.remote_iface_socket:
            self.remote_iface_socket.close()
            self.remote_iface_socket = None

    def clear(self):
        """Remove all registered listeners."""
        self.listeners.clear()

    def remove(self, event_type, listener):
        """Remove listener for specific event.

        Args:
            event_type (EventType) - event type the listener has been registered for
            listener (callable) - listener function
        """
        listener_list = self.listeners.get(event_type)
        filtered_list = [listener for listener in listener_list if listener.caller != listener]

        if len(filtered_list) != listener_list:
            self.listeners[event_type] = filtered_list

    def register(self, event_type, tags, listener, *args, **kwargs):
        """Register listener for specific event type.

        The listener will be called when specific event occur.
        The listener will be called with the event object and defined arguments and keyword arguments.

        Args:
            event_type (EventType) - type of event the listener should be called
            tags (dict) - the tag names and values that should be matched by the event
            listener (callable) - listener function
            args (list) - listener additional args
            kwargs (dict)- listener additional keyword arguments
        """
        self.listeners.setdefault(event_type, []).append(EventListener(listener, tags, *args, **kwargs))
        logging.debug(f'registered listener {listener!r} for event {event_type} - '
                      f'total number of listeners for event {event_type}: {len(self.listeners.get(event_type))}')

    def publish(self, event):
        """Publish event occurence.

        All registered listeners on given event type will be called.

        Args:
            event (Event) - event data
        """
        self.event_queue.put_nowait(event)

    def _call_listeners(self, event, listener_list):
        """Call list of listeners with given event.

        Args:
            event (Event) - event data
            listener_list (list) - list with listeners to call
        """
        for listener in listener_list:
            logging.debug(f'checking tags match of listener {listener!r} for event {event.event_type}')
            if listener.match_tags(event.tags):
                logging.debug(f'listener {listener!r} matches')
                listener.call(event)
            else:
                logging.debug(f'listener {listener!r} doesn\'t match')

    async def _queue_handler(self):
        """Asynchronous event publishing handler."""
        try:
            while True:
                event = await self.event_queue.get()

                logging.info(f'handling event {event.event_type} ({event.data})')

                try:
                    self._call_listeners(event, self.listeners.get(EventType.ALL, []))
                    self._call_listeners(event, self.listeners.get(event.event_type, []))
                finally:
                    self.event_queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            logging.info('finishing event handler')

    async def _remote_iface_handler(self):
        """Listener for the remote events."""
        try:
            while True:
                event = await self.remote_iface_socket.recv_pyobj()
                await self.remote_iface_socket.send_pyobj('ok')

                logging.info(f'handling remote event {event.event_type}')
                try:
                    self.publish(event)
                except:
                    logging.exception('failed to publish remote event')
        except asyncio.CancelledError:
            raise
        finally:
            logging.info('finishing event handler')


class EventPublisherForwarder(EventPublisher):
    def __init__(self, remote_address):
        self.remote_address = remote_address

        super().__init__()

    def clear(self):
        """Remove all registered listeners."""
        raise NotImplementedError()

    def remove(self, event_type, listener):
        """Remove listener for specific event.

        Args:
            event_type (EventType) - event type the listener has been registered for
            listener (callable) - listener function
        """
        raise NotImplementedError()

    def register(self, event_type, tags, listener, *args, **kwargs):
        raise NotImplementedError()

    def start(self):
        """Start event publishing forwarder."""
        self.queue_handler = asyncio.ensure_future(self._queue_forwarder())

    async def _queue_forwarder(self):
        """Asynchronous event publishing handler."""
        out_socket = None

        try:
            out_socket = Context.instance().socket(zmq.REQ) #pylint: disable=maybe-no-member
            out_socket.connect(self.remote_address)

            while True:
                event = await self.event_queue.get()

                logging.info(f'forwarding event {event.event_type} to {self.remote_address}')

                try:
                    await out_socket.send_pyobj(event)
                    # ignore results - we are not interested in reply
                    await out_socket.recv_pyobj()
                except:
                    logging.exception('event forward error')
                finally:
                    self.event_queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            if out_socket:
                out_socket.close()
            logging.info('finishing event forwarder')
