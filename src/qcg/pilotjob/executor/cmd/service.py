import click
import logging
import asyncio
import sys
import signal

from qcg.pilotjob.common.config import Configuration, Var
from qcg.pilotjob.common.zmqiface import ZMQInterface
from qcg.pilotjob.common.receiver import Receiver
from qcg.pilotjob.executor.exhandler import EXHandler
from qcg.pilotjob.executor.exmanager import EXManager
import qcg.pilotjob.executor.events as events
from qcg.pilotjob.common.eventmonitor import EventPublisher


stop_processing = False

def sig_int_handler(sig, frame):
    logging.info('got int signal - finishing')
    global stop_processing
    stop_processing = True


@click.command()
@click.option('-i', '--address', envvar='QCG_IQ_ADDRESS', help="input queue service address")
@click.option('-d', '--debug', envvar='QCG_IQ_DEBUG', is_flag=True, default=False, help="enable debugging")
def ex(address, debug):
    setup_logging(debug)

    logging.info('starting executor service')

    signal.signal(signal.SIGINT, sig_int_handler)

    setup_event_loop()

    if not address:
        iq_address_fname = 'iq.last_service_address'
        with open(iq_address_fname) as iq_address_f:
            address = iq_address_f.read()
        logging.info(f'read iq address: {address}')

    try:
        config = Configuration({Var.IQ_ADDRESS: address, Var.IQ_KEY: 0})
        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(run_service(config)))
    finally:
        tasks = asyncio.Task.all_tasks(asyncio.get_event_loop())
        logging.info(f'#{len(tasks)} all tasks in event loop before closing')
        for idx, task in enumerate(tasks):
            logging.info(f'\ttask {idx}: {str(task)}')

        asyncio.get_event_loop().close()

    logging.info('finito')
    sys.exit(0)


async def run_service(config):
    events.init_event_publisher(EventPublisher(with_remote_iface=True))

    zmq_iface = ZMQInterface()
    zmq_iface.start(config)

    ex_manager = EXManager(config)
    ex_handler = EXHandler(ex_manager)

    receiver = Receiver()
    receiver.start(ex_handler, zmq_iface)

    try:
        await ex_manager.start()

        logging.info('manager started')

        while not stop_processing:
            await asyncio.sleep(2)

        logging.info('got stop processing signal')
    except:
        logging.exception('executor error')
    finally:
        await ex_manager.stop()
        await receiver.stop()
        await events.event_publisher.stop()

    logging.info('service stopped')


def setup_event_loop():
    logging.debug('checking event loop')
    if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
        logging.debug('setting new event loop')
        asyncio.set_event_loop(asyncio.new_event_loop())


def setup_logging(debug):
    level=logging.DEBUG if debug else logging.WARNING

    logging.basicConfig(level=level,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M')


if __name__ == '__main__':
    ex()
