import click
import logging
import asyncio
import signal

from qcg.pilotjob.common.config import Configuration
from qcg.pilotjob.common.zmqiface import ZMQInterface
from qcg.pilotjob.common.receiver import Receiver
from qcg.pilotjob.inputqueue.iqmanager import IQManager
from qcg.pilotjob.inputqueue.iqhandler import IQHandler


stop_processing = False

def sig_int_handler(sig, frame):
    logging.info('got int signal - finishing')
    global stop_processing
    stop_processing = True

@click.command()
@click.option('-d', '--debug', envvar='QCG_IQ_DEBUG', is_flag=True, default=False, help="enable debugging")
def iq(debug):
    setup_logging(debug)

    logging.info('starting input queue service')

    signal.signal(signal.SIGINT, sig_int_handler)

    setup_event_loop()

    try:
        config = Configuration()
        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(run_service(config)))
    finally:
        asyncio.get_event_loop().close()


async def run_service(config):
    zmq_iface = ZMQInterface()
    zmq_iface.start(config)

    iq_address_fname = 'iq.last_service_address'
    with open(iq_address_fname, 'wt') as iq_address_f:
        iq_address_f.write(zmq_iface.external_address)
    logging.debug(f'current iq address saved to {iq_address_fname}')

    manager = IQManager(config)
    iq_handler = IQHandler(manager)

    receiver = Receiver()
    receiver.start(iq_handler, zmq_iface)

    try:
        while not stop_processing:
            await asyncio.sleep(2)
    except:
        logging.exception('run_service error')
    finally:
        await receiver.stop()
        manager.stop()


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
    iq()