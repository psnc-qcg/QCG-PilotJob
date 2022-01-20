from qcg.pilotjob.common.eventmonitor import EventType, Event, EventPublisher, EventPublisherForwarder

import pytest
import asyncio
import logging
from enum import Enum


class MyEvents(Enum):
    EVENT_1 = 1
    EVENT_2 = 2

def event_handler(event, event_metrics):
    logging.info(f'signaled event {event.event_type} with data {event.data} emited at {event.time}')
    event_metrics['cnt'] = event_metrics.get('cnt', 0) + 1

def test_eventmonitor_simple_events(caplog):
    caplog.set_level(logging.DEBUG)
    event_1_metrics = {'cnt': 0}

    loop = asyncio.get_event_loop()
    if loop and loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    publisher = EventPublisher()

    publisher.register(MyEvents.EVENT_1, {}, event_handler, event_1_metrics)

    publisher.publish(Event(MyEvents.EVENT_1, None, None))
    publisher.publish(Event(MyEvents.EVENT_1, None, None))
    publisher.publish(Event(MyEvents.EVENT_2, None, None))

    loop.run_until_complete(asyncio.sleep(0.5))

    assert event_1_metrics.get('cnt') == 2

    loop.run_until_complete(publisher.stop())
    loop.close()

def test_eventmonitor_events_tags(caplog):
    caplog.set_level(logging.DEBUG)
    event_1_metrics = {'cnt': 0}

    loop = asyncio.get_event_loop()
    if loop and loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    publisher = EventPublisher()

    publisher.register(MyEvents.EVENT_1, {'source': 'ex_1'}, event_handler, event_1_metrics)

    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'source': 'ex_1'}))
    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'source': 'ex_2'}))
    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'source': 'ex_3'}))
    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'dest': 'ex_1'}))
    publisher.publish(Event(MyEvents.EVENT_2, None, tags={'source': 'ex_1'}))

    loop.run_until_complete(asyncio.sleep(0.5))

    assert event_1_metrics.get('cnt') == 1

    loop.run_until_complete(publisher.stop())
    loop.close()

def test_eventmonitor_events_tags_all(caplog):
    caplog.set_level(logging.DEBUG)
    event_1_metrics = {'cnt': 0}

    loop = asyncio.get_event_loop()
    if loop and loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    publisher = EventPublisher()

    publisher.register(EventType.ALL, {'source': 'ex_1'}, event_handler, event_1_metrics)
    publisher.register(MyEvents.EVENT_2, {'source': 'ex_2'}, event_handler, event_1_metrics)

    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'source': 'ex_1'})) # count
    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'source': 'ex_2'})) # not count
    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'source': 'ex_3'})) # not count
    publisher.publish(Event(MyEvents.EVENT_1, None, tags={'dest': 'ex_1'})) # not count
    publisher.publish(Event(MyEvents.EVENT_2, None, tags={'source': 'ex_2'})) # count
    publisher.publish(Event(MyEvents.EVENT_2, None, tags={'source': 'ex_3'})) # not count
    publisher.publish(Event(MyEvents.EVENT_2, None, tags={'source': 'ex_1'})) # count

    loop.run_until_complete(asyncio.sleep(0.5))

    assert event_1_metrics.get('cnt') == 3

    loop.run_until_complete(publisher.stop())
    loop.close()

def test_eventmonitor_forwarder(caplog):
    caplog.set_level(logging.DEBUG)
    event_1_metrics = {'cnt': 0}

    loop = asyncio.get_event_loop()
    if loop and loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    publisher = EventPublisher(with_remote_iface=True)
    publisher.register(MyEvents.EVENT_1, None, event_handler, event_1_metrics)

    remote_publisher = EventPublisherForwarder(remote_address=publisher.remote_iface_address)
    remote_publisher.publish(Event(MyEvents.EVENT_1, None))
    remote_publisher.publish(Event(MyEvents.EVENT_1, None))
    remote_publisher.publish(Event(MyEvents.EVENT_2, None)) # not count

    loop.run_until_complete(asyncio.sleep(0.5))

    assert event_1_metrics.get('cnt') == 2

    loop.run_until_complete(remote_publisher.stop())
    loop.run_until_complete(publisher.stop())
    loop.close()

