from qcg.pilotjob.executor.exmanager import EXManager

import pytest
import asyncio
import socket
import zmq
from zmq.asyncio import Context

from qcg.pilotjob.config import Var


async def local_iq_handler(zmq_socket, iq_stats):
    while True:
        request = await zmq_socket.recv_json()
        print(f'local iq received: {request}')

        if request.get('cmd') == 'register':
            iq_stats['registered'] = iq_stats.get('registered', 0) + 1
        elif request.get('cmd') == 'heart_beat':
            iq_stats['heart_beats'] = iq_stats.get('heart_beats', 0) + 1
        elif request.get('cmd') == 'get_iterations':
            iq_stats['get_iterations'] = iq_stats.get('get_iterations', 0) + 1

        await zmq_socket.send_json({'code': 0})
        print(f'local is replied')


def iq_service(iq_stats):
    zmq_ctx = Context.instance()
    zmq_socket = zmq_ctx.socket(zmq.REP)
    zmq_socket.bind_to_random_port('tcp://*')

    external_address = str(bytes.decode(zmq_socket.getsockopt(zmq.LAST_ENDPOINT)))
    if '//0.0.0.0:' in external_address:
        external_address = external_address.replace('//0.0.0.0:', f'//{socket.gethostbyname(socket.gethostname())}:')

    print(f'local IQ external address: {external_address}')
    local_server = asyncio.ensure_future(local_iq_handler(zmq_socket, iq_stats))

    return external_address, local_server


@pytest.mark.asyncio
async def test_exmanager_iface():
    iq_stats = {}
    iq_address, local_server = iq_service(iq_stats)

    config = {
        Var.EXECUTOR_NAME: 'executor_1',
        Var.IQ_ADDRESS: iq_address,
        Var.IQ_KEY: 'secret key',
        Var.RESOURCES: 'local'
    }

    manager = EXManager(config)
    await manager.start()
    await asyncio.sleep(10)
    await manager.stop()

    local_server.cancel()
    try:
        asyncio.wait_for(local_server, 3)
    except Exception:
        pass

    print(f'iq stats at finish: {iq_stats}')
    assert iq_stats.get('registered') == 1, f'wrong number of register events {iq_stats.get("registered")} '\
                                            f'vs expected 1'
    assert iq_stats.get('heart_beats', 0) > 0, f'wrong number of heart beat events {iq_stats.get("heart_beats")} ' \
                                            f'vs expected >0'
    assert iq_stats.get('get_iterations', 0) > 0, f'wrong number of get iterations events '\
                                                  f'{iq_stats.get("get_iterations")} vs expected >0'

