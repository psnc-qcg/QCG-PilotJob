from qcg.pilotjob.common.eventmonitor import Event, EventPublisher
from enum import Enum
import logging


event_publisher = None


class ExEventType(Enum):

    HANDLE_REQUEST = 1 # handle request
    REGISTER_IQ = 2 # send registration to the iq
    IQ_NEW_JOBS = 3 # got notification about new jobs in the iq
    HEARTBEAT_SIGNAL = 4 # send heart beat signal
    REQUEST_NEW_ITS = 5 # send request for new iterations to the iq
    START = 6 # executor manager starts
    STOP = 7 # executor manager stops
    ENQUEUE_ITS = 8 # enqueue new iterations in queue
    EXECUTE_IT = 9 # execute iteration
    STARTED_IT = 10 # execute iteration
    FINISHED_IT = 11 # execute iteration
    SCHEDULE_LOOP_BEGIN = 12 # schedule loop invocation begin
    SCHEDULE_LOOP_END = 13 # schedule loop invocation finish
    IQ_REQUEST_SEND = 14 # sending request to the iq
    IQ_RESPONSE_RECV = 15 # receiving response from the iq
    AGENT_LAUNCH_BEGIN = 16 # launching executor agent
    AGENT_LAUNCH_END = 17 # got reply from launched agent
    AGENT_SHUTDOWN = 18 # send exit message to the agent
    AGENT_APP_START = 19 # send message to agent to launch app
    AGENT_APP_FINISHED = 20 # got message from agent about app finished

class ExEvent(Event):
    def __init__(self, event_type, data, tags):
        super().__init__(event_type, data, tags)

    @staticmethod
    def handle_request(data, tags=None):
        return ExEvent(ExEventType.HANDLE_REQUEST, data, tags)

    @staticmethod
    def register_iq(data, tags=None):
        return ExEvent(ExEventType.REGISTER_IQ, data, tags)

    @staticmethod
    def iq_new_jobs(data, tags=None):
        return ExEvent(ExEventType.IQ_NEW_JOBS, data, tags)

    @staticmethod
    def heartbeat_signal(data, tags=None):
        return ExEvent(ExEventType.HEARTBEAT_SIGNAL, data, tags)

    @staticmethod
    def request_new_its(data, tags=None):
        return ExEvent(ExEventType.REQUEST_NEW_ITS, data, tags)

    @staticmethod
    def start(data, tags=None):
        return ExEvent(ExEventType.START, data, tags)

    @staticmethod
    def stop(data, tags=None):
        return ExEvent(ExEventType.STOP, data, tags)

    @staticmethod
    def enqueue_its(data, tags=None):
        return ExEvent(ExEventType.ENQUEUE_ITS, data, tags)

    @staticmethod
    def execute_it(data, tags=None):
        return ExEvent(ExEventType.EXECUTE_IT, data, tags)

    @staticmethod
    def started_it(data, tags=None):
        return ExEvent(ExEventType.STARTED_IT, data, tags)

    @staticmethod
    def finished_it(data, tags=None):
        return ExEvent(ExEventType.FINISHED_IT, data, tags)

    @staticmethod
    def schedule_loop_begin(data, tags=None):
        return ExEvent(ExEventType.SCHEDULE_LOOP_BEGIN, data, tags)

    @staticmethod
    def schedule_loop_end(data, tags=None):
        return ExEvent(ExEventType.SCHEDULE_LOOP_BEGIN, data, tags)

    @staticmethod
    def agent_launch_begin(data, tags=None):
        return ExEvent(ExEventType.AGENT_LAUNCH_BEGIN, data, tags)

    @staticmethod
    def agent_launch_end(data, tags=None):
        return ExEvent(ExEventType.AGENT_LAUNCH_END, data, tags)

    @staticmethod
    def agent_shutdown(data, tags=None):
        return ExEvent(ExEventType.AGENT_SHUTDOWN, data, tags)

    @staticmethod
    def agent_app_start(data, tags=None):
        return ExEvent(ExEventType.AGENT_APP_START, data, tags)

    @staticmethod
    def agent_app_finish(data, tags=None):
        return ExEvent(ExEventType.AGENT_APP_FINISHED, data, tags)

    def publish(self):
        global event_publisher
        event_publisher.publish(self)


def init_event_publisher(event_pub):
    logging.info(f'initializing executor event publisher with {event_pub!r}')
    global event_publisher
    event_publisher = event_pub
