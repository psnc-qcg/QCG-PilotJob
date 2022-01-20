import click
import logging
import sys
import zmq
import json


@click.group()
@click.option('-d', '--debug', envvar='QCG_CLIENT_DEBUG', is_flag=True, default=False, help="enable debugging")
@click.option('-i', '--address', envvar='QCG_CLIENT_ADDRESS', required=False, help="input queue service address")
@click.pass_context
def client(ctx, debug, address):
    setup_logging(debug)

    if not address:
        iq_address_fname = 'iq.last_service_address'
        with open(iq_address_fname) as iq_address_f:
            address = iq_address_f.read()
        logging.info(f'read iq address: {address}')

    ctx.ensure_object(dict)
    ctx.obj['address'] = address


@client.command()
@click.pass_context
@click.argument('name', nargs=-1)
def status(ctx, name):
    address = ctx.obj.get('address')
    print(f'connecting to {address} ...')

    request_data = {'cmd': 'job_info', 'jobs': name}
    try:
        socket = connect(address)
        socket.send_json(request_data)
        response = socket.recv_json()
        print(f'job info response: {response}')
    except:
        logging.exception('error while geting info')
        raise
    finally:
        disconnect(socket)


@client.command()
@click.pass_context
@click.option('-f', '--file', envvar='QCG_CLIENT_FILE', type=click.File('r'), default=sys.stdin,
              help="read job description from file")
def submit(ctx, file):
    address = ctx.obj.get('address')
    print(f'connecting to {address} ...')

    with file:
        job_desc = json.loads(file.read())

    request_data = {'cmd': 'submit_job', 'jobs': None}

    if isinstance(job_desc, dict):
        request_data['jobs'] = [job_desc]
    elif isinstance(job_desc, list):
        request_data['jobs'] = job_desc
    else:
        raise TypeError(f'error: wrong type of job description {type(job_desc)} - must be list (multiple jobs) '
                        'or dictionary (single job)')

    print(f'submiting job description: {json.dumps(request_data, indent=2)}')

    try:
        socket = connect(address)
        socket.send_json(request_data)
        response = socket.recv_json()
        print(f'jobs submited with response: {response}')
    except:
        logging.exception('error while submiting')
        raise
    finally:
        disconnect(socket)


def setup_logging(debug):
    level=logging.DEBUG if debug else logging.WARNING

    logging.basicConfig(level=level,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M')


def connect(address):
    zmq_ctx = zmq.Context()

    zmq_socket = zmq_ctx.socket(zmq.REQ)  # pylint: disable=maybe-no-member
    zmq_socket.connect(address)
    return zmq_socket


def disconnect(socket):
    if socket:
        socket.close()


if __name__ == '__main__':
    client(obj={})
