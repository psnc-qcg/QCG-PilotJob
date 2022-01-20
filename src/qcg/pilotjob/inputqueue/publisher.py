import logging
import zmq
import socket
import json
import asyncio
import re

from enum import Enum
from zmq.asyncio import Context
from qcg.pilotjob.common.config import Var
from qcg.pilotjob.common.errors import UnknownEventTopic, WrongEventFormat, InternalError


class EventTopic(Enum):
    """Published event types."""

    NEW_JOBS = "NJ"


class Publisher:
    """
    Publish events.

    Publisher handles notifications with queing events to publish. The queing event is done synchronously.
    In the background the sender task takes queued events and sent them (asynchronously).

    Attributes:
        zmq_ctx (Context): ZMQ context
        socket (socket): ZMQ PUB socket
        address (str): address of ZMQ PUB interface from configuration
        external_address (str): address on external network interface (not on private ips)
    """

    def __init__(self):
        """Initialize Publisher."""
        self.zmq_ctx = None
        self.socket = None
        self.address = None
        self.external_address = None

        self.events_queue = None
        self.publisher_task = None

    def setup(self, config):
        """Create Publisher interface.

        If port number is not specified in QCG-PilotJob configuration, it is chosen randomly from configured range.
        """
        self.zmq_ctx = Context.instance()

        self.address = config.get(Var.ZMQ_PUB_ADDRESS)

        self.socket = self.zmq_ctx.socket(zmq.PUB) #pylint: disable=maybe-no-member

        if re.search(r':[0-9]+$', self.address):
            self.socket.bind(self.address)
        else:
            self.socket.bind_to_random_port(self.address,
                                            min_port=int(config.get(Var.ZMQ_PORT_MIN_RANGE)),
                                            max_port=int(config.get(Var.ZMQ_PORT_MAX_RANGE)))

        real_address = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT)))#pylint: disable=maybe-no-member

        # the real address might contain the 0.0.0.0 IP address which means that it listens on all
        # interfaces, sadly this address is not valid for external services to communicate, so we
        # need to replace 0.0.0.0 with the real address IP
        self.external_address = real_address
        if '//0.0.0.0:' in real_address:
            self.external_address = real_address.replace('//0.0.0.0:', '//{}:'.format(
                socket.gethostbyname(socket.gethostname())))

        logging.info(f'status publisher interface configured (address {real_address})'
                     f', external address @ {self.external_address}')

        self.events_queue = asyncio.Queue()
        self.publisher_task = asyncio.ensure_future(self._send_events())

    async def _send_events(self):
        """
        Take enqueued events from queue and send them via socket.
        """
        if not self.events_queue:
            logging.error('publish events queue not initialized')
            raise InternalError('publish event queue not initialized')

        try:
            while True:
                message = await self.events_queue.get()
                logging.info(f'publishing event {message}')
                await self.socket.send_string(message)
        except asyncio.CancelledError:
            logging.info('publisher finish sending events task')
        except:
            logging.exception('publisher error')
        finally:
            logging.info('publisher finish sending events task')


    @staticmethod
    def encode_published_data(topic, data):
        """
        Encode event data to be sent with `send_string` socket method.

        :param topic (EventTopic): the event topic
        :param data (obj): data to sent
        :return: encoded event with topic as string
        """
        return f'{topic.value} {json.dumps(data)}'

    @staticmethod
    def decode_published_data(event_message):
        """
        Decode received event.

        :param event_message: the received message
        :return: tupple (EventTopic, object) with event topic and deserialized data
        :raise:
            UnknownEventTopic - when received topic is not known
            WrongEventFormat - when event data cannot be deserialized
        """
        parts = event_message.partition(' ')
        try:
            topic = EventTopic(parts[0].upper())
        except Exception as exc:
            raise UnknownEventTopic(f'Unknown event topic {parts[0]}: {str(exc)}')

        try:
            data = json.loads(parts[2])
        except Exception as exc:
            raise WrongEventFormat(f'Wrong format of event data {parts[2]}: {str(exc)}')

        return topic, data

    def publish(self, topic, data):
        """
        Publish event

        :param topic (EventTopic): event's topic
        :param data (obj): event data
        """
        if self.events_queue:
            logging.debug(f'enqueuing event {data} with topic {topic}')
            self.events_queue.put_nowait(Publisher.encode_published_data(topic, data))
        else:
            raise InternalError('Publishing task not initialized')

    def stop(self):
        """
        Cleanup.
        """
        logging.info('stopping publisher')
        if self.publisher_task:
            logging.info('stopping publisher task')
            self.publisher_task.cancel()
            try:
                asyncio.wait_for(self.publisher_task, 5)
            except asyncio.TimeoutError:
                logging.error('failed to stop publisher send task')
            except asyncio.CancelledError:
                pass
            finally:
                self.publisher_task = None

        if self.socket:
            self.socket.close()

        self.zmq_ctx = None
        self.socket = None
        self.address = None
        self.external_address = None
