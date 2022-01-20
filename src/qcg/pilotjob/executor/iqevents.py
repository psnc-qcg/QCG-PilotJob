import logging
import zmq
import asyncio

from zmq.asyncio import Context
from qcg.pilotjob.inputqueue.publisher import EventTopic, Publisher


class EventsListener:


    def __init__(self, publisher_address):
        self.publisher_address = publisher_address

        self.topic_callbacks = {}
        self.listener_socket = None
        self.listener_task = None

    def register_topic(self, topic, callback):
        logging.debug(f'registering listener for topic {topic} ...')
        self.topic_callbacks.setdefault(topic, []).append(callback)

    def start(self):
        zmq_ctx = Context.instance()
        self.listener_socket = zmq_ctx.socket(zmq.SUB) # pylint: disable=maybe-no-member
        self.listener_socket.connect(self.publisher_address)

        logging.info(f'created subscription socket for job status changed @ {self.publisher_address}')
        self.listener_socket.setsockopt_string(zmq.SUBSCRIBE, EventTopic.NEW_JOBS.value) # pylint: disable=maybe-no-member

        try:
            self.listener_task = asyncio.ensure_future(self.listen())
        except:
            logging.exception('Failed to start events listener')
            raise

    async def stop(self):
        if self.listener_task:
            logging.info('canceling events listener')

            try:
                self.listener_task.cancel()
            except Exception as exc:
                logging.warning(f'failed to cancel events listener: {str(exc)}')

            try:
                await self.listener_task
            except asyncio.CancelledError:
                logging.debug('events listener canceled')

            self.listener_task = None

        if self.listener_socket:
            self.listener_socket.close()

    async def listen(self):
        logging.info(f'starting listening for events on address {self.publisher_address} ...')

        while True:
            try:
                event = await self.listener_socket.recv_string()
                logging.info(f'got event from {self.publisher_address}: {event}')
                topic, event_data = Publisher.decode_published_data(event)
                if topic in self.topic_callbacks:
                    logging.info(f'found listeners on topic {topic}')
                    asyncio.ensure_future(self.notify_callbacks(self.topic_callbacks.get(topic), event_data))
                else:
                    logging.info(f'not found listeners on topic {topic}')

            except asyncio.CancelledError:
                logging.info(f'finishing listening for events on address {self.publisher_address}')
                break
            except:
                logging.exception(f'error during listening for events on address {self.publisher_address}')

        logging.info(f'events listener task finished')

    async def notify_callbacks(self, callbacks, event_data):
        for callback in callbacks:
            callback(event_data)
