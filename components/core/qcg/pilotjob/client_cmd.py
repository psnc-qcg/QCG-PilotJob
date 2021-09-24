import click
import logging
import re
import traceback
import json
import zmq
import sys

from os.path import exists, join, abspath, isdir
from os import listdir

DEFAULT_PROTO = 'tcp'
DEFAULT_PORT = '21000'

AUX_DIR_PTRN = re.compile(r'\.qcgpjm-service-.*')


_logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass

class ResponseError(Exception):
    pass


def find_aux_dirs(path):
    apath = abspath(path)
    return [join(apath, entry) for entry in listdir(apath) \
            if AUX_DIR_PTRN.match(entry) and isdir(join(apath, entry))]

def check_aux_dir(path):
    auxdirs = find_aux_dirs(path)

    return auxdirs[0] if len(auxdirs) > 0 else None

def get_address_from_directory(path):
    auxdirs = find_aux_dirs(path)

    for wdir in auxdirs:
        if exists(join(wdir, 'address')):
            with open(join(wdir, 'address'), 'r') as f:
                return f.read(), wdir

    return None, None


def get_address_from_arg(address):
    result = address

    if not re.match('\w*://', result):
        # append default protocol
        result = "%s://%s" % (DEFAULT_PROTO, result)

    if not re.match('.*:\d+', result):
        # append default port
        result = "%s:%s" % (result, DEFAULT_PORT)

    return result


def zmq_connect(pjm_ctx):
    _logger.debug('qcg pjm network interface address: {}'.format(str(pjm_ctx['address'])))

    zmq_ctx = {}
    zmq_ctx['ctx'] = zmq.Context.instance()
    zmq_ctx['sock'] = zmq_ctx['ctx'].socket(zmq.REQ)
    zmq_ctx['sock'].setsockopt(zmq.LINGER, 0)
    zmq_ctx['sock'].connect(pjm_ctx['address'])

    _logger.debug('zmq connection created for service @ {}'.format(str(pjm_ctx['address'])))
    return zmq_ctx


def zmq_request(zmq_ctx, request, timeout_secs):
    zmq_ctx['sock'].send(str.encode(json.dumps({
        "request": request
    })))

    _logger.debug('\'{}\' request send - waiting for reponse'.format(request))

    poller = zmq.Poller()
    poller.register(zmq_ctx['sock'], zmq.POLLIN)
    if poller.poll(timeout_secs*1000): # 10s timeout in milliseconds
        response = zmq_ctx['sock'].recv_json()
    else:
        raise TimeoutError('Timeout processing status request')

    return response


def validate_response(response):
    if response['code'] != 0:
        raise ResponseError('Request failed: {}'.format(response.get('message', 'error')))

    return response.get('data', {})


@click.group()
@click.option('-p', '--path', envvar='QCG_PJM_DIR', help="path to the QCG-PJM working directory")
@click.option('-a', '--address', envvar='QCG_PJM_ADDRESS', help="address of the QCG-PJM network interface")
@click.option('-d', '--debug', envvar='QCG_CLIENT_DEBUG', is_flag=True, default=False, help="enable debugging - all messages will be written to the local qcgclient.log file")
@click.pass_context
def qcgpjm(ctx, path, address, debug):
    """QCG PJM command line client."""
    try:
        setup_logging(debug)

        pjm_path = None

        if address:
            pjm_address = get_address_from_arg(address)
        elif path:
            (pjm_address, pjm_path) = get_address_from_directory(path)
        else:
            (pjm_address, pjm_path) = get_address_from_directory('.')

        if not pjm_address:
            raise Exception('unable to find QCG PJM network interface address (use \'-p\' or \'-a\' argument)')

        pjm_ctx = { 'path': pjm_path, 'address': pjm_address }
        _logger.debug('qcg pjm network interface address: {}'.format(str(pjm_ctx['address'])))

        ctx.ensure_object(dict)
        ctx.obj['pjm'] = pjm_ctx
    except Exception as e:
        click.echo('error: {}'.format(str(e)), err=True)
        _logger.error(traceback.format_exc())
        sys.exit(1)


@qcgpjm.command()
@click.pass_context
def status(ctx):
    """Show service status."""
    try:
        pjm_ctx = ctx.obj['pjm']

        zmq_ctx = zmq_connect(pjm_ctx)

        try:
            response = zmq_request(zmq_ctx, 'status', 5)
        except TimeoutError:
            # try to read final status - if exists
            if pjm_ctx.get('path') and exists(join(pjm_ctx['path'], 'final_status')):
                _logger.debug('reading final status from file {}'.format(join(pjm_ctx['path'], 'final_status')))
                with open(join(pjm_ctx['path'], 'final_status')) as f:
                    response = json.loads(f.read())
            else:
                _logger.info('failed to find the final status file in path {}'.format(pjm_ctx.get('path')))
                raise

        status = validate_response(response)
        
        for secname, secdata in status.items():
            print('[{}]'.format(secname))
            for key, value in secdata.items():
                print('{}={}'.format(key, value))
    except Exception as e:
        click.echo('error: {}'.format(str(e)), err=True)
        _logger.error(traceback.format_exc())



def setup_logging(debug):
    level=logging.DEBUG if debug else logging.WARNING

    auxdir = check_aux_dir('.')
    if auxdir:
        logfile = join(auxdir, 'qcgpjmclient.log')
    else:
        logfile = 'qcgpjmclient.log'

    logging.basicConfig(level=level,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=logfile,
                        filemode='w')


if __name__ == '__main__':
    qcgpjm(obj={})
