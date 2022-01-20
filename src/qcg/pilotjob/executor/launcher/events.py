from qcg.pilotjob.common.eventmonitor import Event
from enum import Enum


class ExAgentEventType(Enum):
    PROCESS_LAUNCH = 100 + 1 # new process launched
    PROCESS_FINISH = 100 + 2 # process finished

# must be initialized before first event publish either with `common.eventmonitor.EventPublisher` or
# `common.eventmonitor.EventPublisherForwarder`
event_publisher = None

def init_event_publisher(event_pub):
    global event_publisher
    event_publisher = event_pub

class ExAgentEvent(Event):
    def __init__(self, event_type, data, tags):
        super().__init__(event_type, data, tags)

    @staticmethod
    def process_launch(data, tags=None):
        return ExAgentEvent(ExAgentEventType.PROCESS_LAUNCH, data, tags)

    @staticmethod
    def process_finish(data, tags=None):
        return ExAgentEvent(ExAgentEventType.PROCESS_FINISH, data, tags)

    def publish(self):
        event_publisher.publish(self)


