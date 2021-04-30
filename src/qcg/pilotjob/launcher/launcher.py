import asyncio
import json
import sys
import logging
import shutil
import socket
import os
from datetime import datetime

import zmq
from zmq.asyncio import Context

from qcg.pilotjob import logger as top_logger


_logger = logging.getLogger(__name__)


class Launcher:
    """The launcher service used to launch applications on remote nodes.

    All nodes should have shared file system.

    Attributes:
        work_dir (str): path to the working directory
        aux_dir (str): path to the auxilary directory
        zmq_ctx (zmq.Context): ZMQ context
        agents (dict): = requested agent instances
        nodes (dict): registered agent instances
        jobs_def_cb (def): application finish default callback
        jobs_cb (dict): application finish callbacks
        node_local_agent_cmd (list): list of command arguments to start agent on local node
        node_ssh_agent_cmd (list): list of command arguments to start agent on remote node via ssh
        in_socket (zmq.Socket):
        local_address (str):
        local_export_address (str):
        iface_task (asyncio.Future):
    """

    MIN_PORT_RANGE = 10000
    MAX_PORT_RANGE = 40000
    # due to eagle node wake up issues
    START_TIMEOUT_SECS = 600
    SHUTDOWN_TIMEOUT_SECS = 30

    MAXIMUM_CONCURRENT_CONNECTIONS = 1000

    def __init__(self, config, wdir, aux_dir):
        """Initialize instance.

        Args:
            config (dict): configuration dictionary
            wdir (str): path to the working directory (the same on all nodes)
            aux_dir (str): path to the auxilary directory (the same on all nodes)
        """
        # config
        self.config = config

        # working directory
        self.work_dir = wdir

        # auxiliary directory
        self.aux_dir = aux_dir

        # local context
        self.zmq_ctx = {}

        # requested agent instances
        self.agents = {}

        # registered agent instances
        self.nodes = {}

        # application finish default callback
        self.jobs_def_cb = None

        # application finish callbacks
        self.jobs_cb = {}

        self.node_local_agent_cmd = [sys.executable, '-m', 'qcg.pilotjob.launcher.agent']
        self.node_ssh_agent_cmd = ['cd {}; {} -m qcg.pilotjob.launcher.agent'.format(self.work_dir, sys.executable)]

        self.in_socket = None
        self.local_address = None
        self.local_export_address = None
        self.iface_task = None

        self.connection_sem = asyncio.Semaphore(Launcher.MAXIMUM_CONCURRENT_CONNECTIONS)

    def set_job_finish_callback(self, jobs_finish_cb, *jobs_finish_cb_args):
        """Set default function for notifing about finished jobs.

        Args:
            jobs_finish_cb - optional default job finish callback
            jobs_finish_cb_args - job finish callback parameters
        """
        if jobs_finish_cb:
            self.jobs_def_cb = {'f': jobs_finish_cb, 'args': jobs_finish_cb_args}
        else:
            self.jobs_def_cb = None

    async def start(self, instances, local_port=None):
        """Initialize launcher with given agent instances.
        The instances data must contain list of places along with the data needed to
        initialize instance services.

        Args:
            instances ({} []) - list of dictionaries where each element must contain fields:
                agent_id - the agent identifier
                ssh - if the instance should be run via ssh (account?,host?,remote_python?)
                slurm - if the instance should be run via slurm ()
                local - if the instance should be run on a local machine
            local_port - optional local port for the incoming connections, if not defined,
                the available random port will be chosen from the range
        """
        try:
            await self._init_input_iface(local_port=local_port)
            _logger.debug('local manager listening at %s', self.local_address)

            await self.__fire_agents(instances)
        except Exception as exc:
            _logger.error('failed to start agents: %s', str(exc))
            await self._cleanup()
            raise

    async def stop(self):
        """Stop all agents and release resources."""
        await self._cleanup()

    async def submit(self, agent_id, app_id, jname, args, stdin=None, stdout=None, stderr=None, env=None, wdir=None,
                     cores=None, finish_cb=None, finish_cb_args=None):
        """Submit application to be launched by the selected agent.

        Args:
            agent_id - agent that should launch applicaton
            app_id - application identifier
            jname - job name
            args - aplication arguments
            stdin - path to the standard input file
            stdout - path to the standard output file
            stderr - path to the standard error file
            env - environment variables (if not defined, the current environment is used)
            wdir - working directory (if not defined, the current working directory is used)
            cores - the list of cores application should be binded to (if not defined, there will be no binding)
            finish_cb - job finish callback
            finish_cb_args - job finish callback arguments
        """
        if agent_id not in self.agents:
            _logger.error('agent %s not registered', agent_id)
            raise Exception('agent {} not registered'.format(agent_id))

        if finish_cb:
            self.jobs_cb[app_id] = {'f': finish_cb, 'args': finish_cb_args}

        agent = self.nodes[agent_id]

        await self.connection_sem.acquire()
        try:
            socket_open_attempts = 0
            while True:
                try:
                    out_socket = self.zmq_ctx.socket(zmq.REQ) #pylint: disable=maybe-no-member
                    break
                except zmq.ZMQError:
                    _logger.info('too many connections while communicating with launcher agent')
                    if socket_open_attempts > 5:
                        raise Exception('failed to communicate with agent - too many connections in the same time')
                    await asyncio.sleep(0.1)
                    socket_open_attempts += 1

            out_socket.connect(agent['address'])

            await out_socket.send_json({
                'cmd': 'RUN',
                'appid': app_id,
                'jname': jname,
                'args': args,
                'stdin': stdin,
                'stdout': stdout,
                'stderr': stderr,
                'env': env,
                'wdir': wdir,
                'cores': cores})
            msg = await out_socket.recv_json()

            if not msg.get('status', None) == 'OK':
                _logger.error('failed to run application %s by agent %s: %s', app_id, agent_id, str(msg))

                if finish_cb:
                    del self.jobs_cb[app_id]

                raise Exception('failed to run application {} by agent {}: {}'.format(app_id, agent_id, str(msg)))

            _logger.debug('application %s successfully launched by agent %s', app_id, agent_id)
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    # ignore errors in this place
                    pass

            self.connection_sem.release()

    async def cancel(self, agent_id, app_id):
        """Cancel sumited application by the selected agent.

        Args:
            agent_id - agent that launched applicaton
            app_id - application identifier
        """
        if agent_id not in self.agents:
            _logger.error(f'agent {agent_id} not registered')
            raise Exception(f'agent {agent_id} not registered')

        agent = self.nodes[agent_id]

        _logger.debug(f'sending cancel app ({app_id}) to agent ({agent_id})')
        out_socket = self.zmq_ctx.socket(zmq.REQ) #pylint: disable=maybe-no-member
        try:
            out_socket.connect(agent['address'])

            await out_socket.send_json({
                'cmd': 'CANCEL',
                'appid': app_id})
            msg = await out_socket.recv_json()

            if not msg.get('status', None) == 'OK':
                _logger.error(f'failed to cancel application {app_id} by agent {agent_id}: {str(msg)}')

                raise Exception(f'failed to run application {app_id} by agent {agent_id}: {str(msg)}')

            _logger.debug(f'application {app_id} successfully canceled by agent {agent_id}')
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    # ignore errors in this place
                    pass

    async def _cleanup(self):
        """ Release all resources.
        Stop all agents, cancel all async tasks and close sockets.
        """
        # signal node agent to shutdown
        try:
            await self._shutdown_agents()
        except Exception:
            pass

        # kill agent processes
        await self._cancel_agents()
        _logger.debug('launcher agents canceled')

        # cancel input interface task
        if self.iface_task:
            self.iface_task.cancel()

            try:
                await self.iface_task
                _logger.debug('launcher iface receiver closed')
            except asyncio.CancelledError:
                _logger.debug('input interface task finished')

        # close input interface socket
        if self.in_socket:
            self.in_socket.close()

        _logger.debug('launcher iface socket closed')

    async def _shutdown_agents(self):
        """Signal all running node agents to shutdown and wait for notification about finishing."""
        out_socket = self.zmq_ctx.socket(zmq.REQ) #pylint: disable=maybe-no-member

        try:
            # create copy because elements might be removed when signal finishing
            nodes = self.nodes.copy()

            for agent_id, agent in nodes.items():
                _logger.debug('connecting to node agent %s @ %s ...', agent_id, agent['address'])

                try:
                    out_socket.connect(agent['address'])

                    await out_socket.send_json({'cmd': 'EXIT'})

                    msg = await out_socket.recv_json()

                    if not msg.get('status', None) == 'OK':
                        _logger.error('failed to finish node agent %s: %s', agent_id, str(msg))
                    else:
                        _logger.debug('node agent %s signaled to shutdown', agent_id)

                    out_socket.disconnect(agent['address'])
                except Exception as exc:
                    _logger.error('failed to signal agent to shutdown: %s', str(exc))

            start_t = datetime.now()

            while len(self.nodes) > 0:
                if (datetime.now() - start_t).total_seconds() > Launcher.SHUTDOWN_TIMEOUT_SECS:
                    _logger.error('timeout while waiting for agents exit - currenlty not finished %d: %s',
                                  len(self.nodes), str(','.join(self.nodes.keys())))
                    raise Exception('timeout while waiting for agents exit')

                await asyncio.sleep(0.2)
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    # ingore errors in this place
                    pass

    async def _cancel_agents(self):
        """Kill agent processes. """
        for agent_id, agent in self.agents.items():
            if agent.get('process', None):
                _logger.debug('killing agent %s ...', agent_id)
                try:
                    await asyncio.wait_for(agent['process'].wait(), 30)
                    agent['process'] = None
                except Exception:
                    _logger.warning('Failed to kill agent: %s', str(sys.exc_info()))
                finally:
                    try:
                        if agent['process']:
                            agent['process'].terminate()
                    except Exception:
                        # ignore errors in this place
                        pass

        self.agents.clear()

    async def __fire_agents(self, instances):
        """Launch node agent instances.

        The input interface must be running to get notification about started agents.

        Args:
            instances (list): specification for agents to launch
        """
        for idata in instances:
            if not all(('agent_id' in idata, 'ssh' in idata or 'slurm' in idata or 'local' in idata)):
                raise ValueError('insufficient agent instance data: {}'.format(str(idata)))

            if idata['agent_id'] in self.agents:
                raise KeyError('agnet instance {} already defined'.format(idata['agent_id']))

            proto = 'ssh' if 'ssh' in idata else None
            proto = 'slurm' if 'slurm' in idata else None
            if not proto:
                proto = 'local'

            _logger.info('running node agent %s via %s', idata['agent_id'], proto)

            agent_args = [idata['agent_id'], self.local_export_address]
            if idata.get('options'):
                agent_args.append(json.dumps(idata['options']))

            if 'ssh' in idata:
                process = await self.__fire_ssh_agent(idata['ssh'], agent_args)
            elif 'slurm' in idata:
                process = await self._fire_slurm_agent(idata['slurm'], agent_args)
            elif 'local' in idata:
                process = await self._fire_local_agent(idata['local'], agent_args)
            else:
                raise ValueError('missing start type of node agent {}'.format(idata['agent_id']))

            self.agents[idata['agent_id']] = {'process': process, 'data': idata}

        start_t = datetime.now()

        while len(self.nodes) < len(self.agents):
            if (datetime.now() - start_t).total_seconds() > Launcher.START_TIMEOUT_SECS:
                _logger.error('timeout while waiting for agents - currenlty registered %s from launched %s',
                              len(self.nodes), len(self.agents))
                _logger.error(f'not registered instances: ')
                for agent_id, agent in self.agents.items():
                    if agent_id not in self.nodes:
                        _logger.error(f'{agent_id}: process ({agent["process"]}), data ({agent["data"]})')
                raise Exception('timeout while waiting for agents')

            await asyncio.sleep(0.2)

        _logger.debug('all agents %d registered in %d seconds', len(self.nodes),
                      (datetime.now() - start_t).total_seconds())

    async def __fire_ssh_agent(self, ssh_data, args):
        """Launch node agent instance via ssh.

        Args:
            ssh_data (dict) - must contain 'host' attribute, optional attributes: 'account' (str), 'args' (str[])
            args - aguments for node agent application
        """
        if 'host' not in ssh_data:
            raise ValueError('missing ssh host definition')

        ssh_address = ssh_data['host']
        if 'account' in ssh_data:
            ssh_address = '{}@{}'.format(ssh_data['account'], ssh_address)

        ssh_args = ssh_data.get('args', [])

        return await asyncio.create_subprocess_exec(shutil.which('ssh'), ssh_address, *ssh_args,
                                                    *self.node_ssh_agent_cmd, *args)

    async def _fire_slurm_agent(self, slurm_data, args):
        """Launch node agent instance via slurm (inside allocation).

        Args:
            slurm_data (dict) - must contain 'node' attribute, optional attributes: 'node' (str), 'args' (str[])
            args - aguments for node agent application
        """
        if 'node' not in slurm_data:
            raise ValueError('missing slurm node name')

        slurm_args = ['-J', 'agent-{}'.format(slurm_data['node']), '-w', slurm_data['node'], '--cpu-bind=none', '-vvv',
                      '--mem-per-cpu=0', '--oversubscribe', '--overcommit', '-N', '1', '-n', '1', '-D', self.work_dir,
                      '-u']

        if top_logger.level == logging.DEBUG:
            slurm_args.extend(['--slurmd-debug=verbose', '-vvvvv'])

        slurm_args.extend(slurm_data.get('args', []))

        stdout_p = asyncio.subprocess.DEVNULL
        stderr_p = asyncio.subprocess.DEVNULL

        if top_logger.level == logging.DEBUG:
            stdout_p = open(os.path.join(self.aux_dir, 'nl-{}-start-agent-stdout.log'.format(slurm_data['node'])), 'w')
            stderr_p = open(os.path.join(self.aux_dir, 'nl-{}-start-agent-stderr.log'.format(slurm_data['node'])), 'w')

        _logger.debug('running agent process with args: %s', ' '.join(
            [shutil.which('srun')] + slurm_args + self.node_local_agent_cmd + args))

        return await asyncio.create_subprocess_exec(shutil.which('srun'), *slurm_args,
                                                    *self.node_local_agent_cmd, *args, stdout=stdout_p, stderr=stderr_p)

    async def _fire_local_agent(self, local_data, args): #pylint: disable=W0613
        """Launch node agent instance locally.

        Args:
            local_data (dict):
            args - aguments for node agent application
        """
        return await asyncio.create_subprocess_exec(*self.node_local_agent_cmd, *args)

    async def _init_input_iface(self, local_port=None, proto='tcp', ip_addr='0.0.0.0', port_min=MIN_PORT_RANGE,
                                port_max=MAX_PORT_RANGE):
        """Initialize input interface.
        The ZMQ socket is created and binded for incoming notifications (from node agents).

        Args:
            local_port - optional local port to bind to
            proto - optional protocol for ZMQ socket
            ip_addr - optional local IP address
            port_min - optional minimum range port number if local_port has not been defined
            port_max - optional maximum range port number if local port has not been defined
        """
        self.zmq_ctx = Context.instance()
        self.in_socket = self.zmq_ctx.socket(zmq.REP) #pylint: disable=maybe-no-member

        if local_port:
            address = '{}://{}:{}'.format(proto, ip_addr, local_port)
            port = self.in_socket.bind(address)
        else:
            addr = '{}://{}'.format(proto, ip_addr)
            port = self.in_socket.bind_to_random_port(addr, min_port=port_min, max_port=port_max)
            address = '{}:{}'.format(addr, port)

        self.local_address = address

        local_hostname = socket.gethostname()

        if os.getenv('SLURM_CLUSTER_NAME', 'unknown') == 'supermucng':
            logging.info(f'modifing launcher export address for supermucng')
            local_opa_hostname='.'.join(elem + '-opa' if it == 0 else elem for it,elem in enumerate(local_hostname.split('.')))
            _logger.info(f'local hostname: {local_hostname}')
            _logger.info(f'local modified hostname: {local_opa_hostname}')

            _logger.info(f'local hostname ip address: {socket.gethostbyname(local_hostname)}')
            _logger.info(f'local modified hostname ip address: {socket.gethostbyname(local_opa_hostname)}')
            local_hostname = local_opa_hostname

        self.local_export_address = '{}://{}:{}'.format(proto, socket.gethostbyname(local_hostname), port)

        _logger.debug('local zmq address accessible by other machines: %s', self.local_export_address)

        self.iface_task = asyncio.ensure_future(self._input_iface())

    async def _input_iface(self):
        """The handler for input interface.
        The incoming notifications are processed and launcher state is modified.
        """
        while True:
            msg = await self.in_socket.recv_json()

            _logger.debug('received message: %s', str(msg))

            if msg.get('status', None) == 'READY':
                try:
                    if not all(('agent_id' in msg, 'local_address' in msg)):
                        raise Exception('missing identifier/local address in ready message')
                except Exception as exc:
                    _logger.error('error: %s', str(exc))
                    await self.in_socket.send_json({'status': 'ERROR', 'message': str(exc)})
                    next
                else:
                    await self.in_socket.send_json({'status': 'CONFIRMED'})

                self.nodes[msg['agent_id']] = {'registered_at': datetime.now(), 'address': msg['local_address']}
                _logger.debug('registered at (%s) agent (%s) listening at (%s)',
                              self.nodes[msg['agent_id']]['registered_at'],
                              msg['agent_id'], self.nodes[msg['agent_id']]['address'])
            elif msg.get('status', None) in ['APP_FINISHED', 'APP_FAILED']:
                await self.in_socket.send_json({'status': 'CONFIRMED'})

                appid = msg.get('appid', 'UNKNOWN')

                _logger.debug('received notification with application %s finish %s', appid, str(msg))

                if appid in self.jobs_cb:
                    try:
                        args = (*self.jobs_cb[appid]['args'], msg) if self.jobs_cb[appid]['args'] else (msg)
                        _logger.debug('calling application finish callback with object %s and args %s',
                                      self.jobs_cb[appid]['f'], str(args))
                        self.jobs_cb[appid]['f'](args)
                    except Exception as exc:
                        _logger.error('failed to call application finish callback: %s', str(exc))

                    del self.jobs_cb[appid]
                elif self.jobs_def_cb:
                    _logger.debug('calling application default finish callback with object %s', self.jobs_def_cb['f'])
                    try:
                        args = (*self.jobs_def_cb['args'], msg) if self.jobs_def_cb['args'] else (msg)
                        self.jobs_def_cb['f'](args)
                    except Exception as exc:
                        _logger.error('failed to call application default finish callback: %s', str(exc))
                else:
                    _logger.debug('NOT calling application finish callback')
            elif msg.get('status', None) == 'FINISHING':
                await self.in_socket.send_json({'status': 'CONFIRMED'})

                _logger.debug('received notification with node agent finish: %s', str(msg))

                if 'agent_id' in msg and msg['agent_id'] in self.nodes:
                    _logger.debug('removing node agent %s', msg['agent_id'])
                    del self.nodes[msg['agent_id']]
                else:
                    _logger.error('node agent %s notifing FINISH not known', msg.get('agent_id', 'UNKNOWN'))
            else:
                await self.in_socket.send_json({'status': 'ERROR'})


def finish_callback_default(text, message):
    """Just for testing."""
    print('got default notifinication with job finish ({}): {}'.format(text, str(message)))


def finish_callback(text, jobid, message):
    """Just for testing."""
    print('got notifinication with job {} finish ({}): {}'.format(jobid, text, str(message)))


async def run_job(launcher, agent_id, appid, args, stdin=None, stdout=None, stderr=None, env=None):
    """Just for testing."""
    # await launcher.submit(agent_id, appid, args, wdir='tmp-dir', finish_cb=finish_callback,
    #   finish_cb_args=['job finish callback', 'job_ID'])
    await launcher.submit(agent_id, appid, args, stdin=stdin, stdout=stdout, stderr=stderr, env=env, wdir='tmp-dir',
                          cores=[0, 1], finish_cb=finish_callback, finish_cb_args=['job finish callback', 'job_ID'])
    # await launcher.submit(agent_id, appid, args)


async def test():
    """Some simple test"""
    logging.basicConfig(level=logging.DEBUG)

    launcher = Launcher('.', '.')

    launcher.set_job_finish_callback(finish_callback_default, 'job finish callback')

#    await launcher.start([{ 'agent_id': 'n1', 'local': { }}])
#    await launcher.start([{ 'agent_id': 'n1', 'ssh': { 'host': 'agave13' }}])
    await launcher.start([
        {'agent_id': 'n1', 'slurm': {'node': 'e0101'}},
        {'agent_id': 'n2', 'slurm': {'node': 'e0103'}}])

    try:
        await run_job(launcher, 'n1', 'job1', ['/usr/bin/cat'], stdin='/etc/system-release', stdout='env.out',
                      stderr='env.err', env={'v1': 'V1_value', 'v2': ''})
        await run_job(launcher, 'n2', 'job2', ['/usr/bin/cat'], stdin='/etc/system-release', stdout='env2.out',
                      stderr='env2.err', env={'v1': 'V1_value', 'v2': ''})
    except Exception as exc:
        _logger.error(str(exc))

    await asyncio.sleep(5)

    await launcher.stop()


if __name__ == '__main__':
    # if len(sys.argv) > 2:
    #     print('error: wrong arguments\n\n\tlauncher {local_address}\n\n')
    #     sys.exit(1)

    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(test()))
