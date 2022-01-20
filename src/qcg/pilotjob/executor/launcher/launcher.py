import asyncio
import json
import sys
import logging
import shutil
import socket
import os
from datetime import datetime
from qcg.pilotjob.executor.events import ExEvent

import zmq
from zmq.asyncio import Context

from qcg.pilotjob.common.resources import ResourcesType


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

    def __init__(self, resources_type, node_names, wdir, aux_dir, agent_options):
        """Initialize instance.

        Args:
            resources_type (ResourcesType): type of resources, determines method of starting agents
            node_names (list(str)): name of the nodes where agens should be launched
            wdir (str): path to the working directory (the same on all nodes)
            aux_dir (str): path to the auxilary directory (the same on all nodes)
            agent_options (dict): directory with options that should be passed to the agents
        """
        # type of resources
        self.resources_type = resources_type
        self.agent_launch_method = {
            ResourcesType.LOCAL: self._fire_local_agent,
            ResourcesType.SLURM: self._fire_slurm_agent
        }[self.resources_type]

        # working directory
        self.work_dir = wdir

        # auxiliary directory
        self.aux_dir = aux_dir

        # local context
        self.zmq_ctx = {}

        # node names
        self.node_names = node_names

        # agent options
        self.agent_options = agent_options

        # requested agent instances
        self.agents = {}

        # registered agent instances
        self.nodes = {}

        # application finish default callback
        self.jobs_def_cb = None

        # application finish callbacks
        self.jobs_cb = {}

        self.node_local_agent_cmd = [sys.executable, '-m', 'qcg.pilotjob.executor.launcher.agent']

        self.in_socket = None
        self.local_address = None
        self.local_export_address = None
        self.iface_task = None


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

    async def start(self):
        """Initialize launcher with given agent instances.
        The instances of agent will be launched on all nodes.
        """
        try:
            await self._init_input_iface()
            logging.debug('local manager listening at %s', self.local_address)

            await self._fire_agents()
        except Exception as exc:
            logging.error('failed to start agents: %s', str(exc))
            await self._cleanup()
            raise

    async def stop(self):
        """Stop all agents and release resources."""
        await self._cleanup()

    async def submit(self, node_name, executor_job, finish_cb=None, finish_cb_args=None):
        """Submit application to be launched by the selected agent.

        Args:
            node_name (str) - the node name where is launcher agent that should recieve this job
            executor_job (qcg.pilotjob.executor.executor.ExecutorJob) - the job iteration to launch
            finish_cb - job finish callback
            finish_cb_args - job finish callback arguments
        """
        if node_name not in self.agents:
            raise Exception(f'node {node_name} not initialized')

        if finish_cb:
            self.jobs_cb[executor_job.id] = {'f': finish_cb, 'args': finish_cb_args}

        agent = self.nodes[node_name]

        out_socket = self.zmq_ctx.socket(zmq.REQ) #pylint: disable=maybe-no-member
        try:
            out_socket.connect(agent['address'])

            logging.info(f'sending RUN request to agent {node_name} with application {executor_job.id} with '
                         f'allocation {executor_job.allocation.export()}')

            ExEvent.agent_app_start({'appid': executor_job.id, 'iteration': executor_job.iteration,
                                     'node': node_name}).publish()
            await out_socket.send_json({
                'cmd': 'RUN',
                'appid': executor_job.id,
                'description': executor_job.job_iterations.description.to_dict(),
                'attrs': executor_job.job_iterations.attributes,
                'iteration': executor_job.iteration,
                'allocation': executor_job.allocation.export(),
            })
            msg = await out_socket.recv_json()

            if not msg.get('status', None) == 'OK':
                logging.error(f'failed to run application {executor_job.id} by agent {node_name}: {msg}')

                if finish_cb:
                    del self.jobs_cb[executor_job.id]

                raise Exception(f'failed to run application {executor_job.id} by agent {node_name}: {msg}')

            logging.debug(f'application {executor_job.id} successfully launched by agent {node_name}')
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
        logging.debug('shuting down executor agents')
        try:
            await self._shutdown_agents()
        except Exception:
            pass

        # kill agent processes
        await self._cancel_agents()
        logging.debug('launcher agents canceled')

        # cancel input interface task
        if self.iface_task:
            logging.info('canceling input iface task')

            try:
                self.iface_task.cancel()
            except Exception as exc:
                logging.warning(f'failed to cancel launcher iface task: {str(exc)}')

            try:
                await self.iface_task
                logging.debug('launcher iface task closed')
            except asyncio.CancelledError:
                logging.debug('input interface task finished')

            self.iface_task = None

        # close input interface socket
        if self.in_socket:
            self.in_socket.close()

        logging.debug('launcher iface socket closed')

    async def _shutdown_agents(self):
        """Signal all running node agents to shutdown and wait for notification about finishing."""
        out_socket = self.zmq_ctx.socket(zmq.REQ) #pylint: disable=maybe-no-member

        try:
            # create copy because elements might be removed when signal finishing
            nodes = self.nodes.copy()

            for agent_id, agent in nodes.items():
                logging.debug('connecting to node agent %s @ %s ...', agent_id, agent['address'])

                try:
                    out_socket.connect(agent['address'])

                    try:
                        ExEvent.agent_shutdown({'node': agent_id}).publish()
                        await asyncio.wait_for(out_socket.send_json({'cmd': 'EXIT'}), 5)
                    except asyncio.TimeoutError:
                        logging.info(f'failed to send EXIT command to agent {agent["address"]}')
                        raise

                    try:
                        msg = await asyncio.wait_for(out_socket.recv_json(), 5)
                    except asyncio.TimeoutError:
                        logging.error(f'failed to receive EXIT command response from agent {agent["address"]}')

                    if not msg.get('status', None) == 'OK':
                        logging.error('failed to finish node agent %s: %s', agent_id, str(msg))
                    else:
                        logging.debug('node agent %s signaled to shutdown', agent_id)

                    out_socket.disconnect(agent['address'])
                except Exception as exc:
                    logging.error('failed to signal agent to shutdown: %s', str(exc))

            start_t = datetime.now()

            while len(self.nodes) > 0:
                if (datetime.now() - start_t).total_seconds() > Launcher.SHUTDOWN_TIMEOUT_SECS:
                    logging.error('timeout while waiting for agents exit - currenlty not finished %d: %s',
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
                logging.debug('killing agent %s ...', agent_id)
                try:
                    await asyncio.wait_for(agent['process'].wait(), 5)
                    agent['process'] = None
                except Exception:
                    logging.warning('Failed to kill agent: %s', str(sys.exc_info()))
                finally:
                    try:
                        if agent['process']:
                            agent['process'].terminate()
                    except Exception:
                        # ignore errors in this place
                        pass

        self.agents.clear()

    async def _fire_agents(self):
        """Launch node agent instances.

        The input interface must be running to get notification about started agents.
        """
        for node_name in self.node_names:
            logging.info(f'running node agent on node {node_name}')

            agent_args = [node_name, self.local_export_address]
            if self.agent_options:
                agent_args.append(json.dumps(self.agent_options))

            process = await self.agent_launch_method(node_name, agent_args)
            self.agents[node_name] = {'process': process}

        start_t = datetime.now()

        while len(self.nodes) < len(self.agents):
            if (datetime.now() - start_t).total_seconds() > Launcher.START_TIMEOUT_SECS:
                logging.error(f'timeout while waiting for agents - currenlty registered {len(self.nodes)}'
                              f'from launched {len(self.agents)}')
                raise Exception('timeout while waiting for agents')

            await asyncio.sleep(0.2)

        logging.debug(f'all {len(self.nodes)} agents registered in {(datetime.now() - start_t).total_seconds()} '
                      'seconds')

    async def _fire_local_agent(self, node_name, args):
        """Launch node agent instance via slurm (inside allocation).

        Args:
            node_name - the node name where to launch agent
            args - aguments for node agent application
        """
        stdout_p = asyncio.subprocess.DEVNULL
        stderr_p = asyncio.subprocess.DEVNULL

        if logging.root.level == logging.DEBUG:
            stdout_p = open(os.path.join(self.aux_dir, f'nl-{node_name}-start-agent-stdout.log'), 'w')
            stderr_p = open(os.path.join(self.aux_dir, f'nl-{node_name}-start-agent-stderr.log'), 'w')

        cmd = ' '.join(self.node_local_agent_cmd + [f"'{arg}'" for arg in args])
        logging.debug(f'running locally agent process with args: {cmd}')

        ExEvent.agent_launch_begin({'node': node_name, 'type': 'local'}).publish()
        return await asyncio.create_subprocess_shell(f'(cd {self.work_dir} && {cmd})', stdout=stdout_p, stderr=stderr_p)

    async def _fire_slurm_agent(self, node_name, args):
        """Launch node agent instance via slurm (inside allocation).

        Args:
            node_name - the node name where to launch agent
            args - aguments for node agent application
        """
        slurm_args = ['-J', 'agent-{}'.format(node_name), '-w', node_name, '--cpu-bind=none', '-vvv',
                      '--mem-per-cpu=0', '--oversubscribe', '--overcommit', '-N', '1', '-n', '1', '-D', self.work_dir,
                      '-u']

        if logging.root.level == logging.DEBUG:
            slurm_args.extend(['--slurmd-debug=verbose', '-vvvvv'])

        stdout_p = asyncio.subprocess.DEVNULL
        stderr_p = asyncio.subprocess.DEVNULL

        if logging.root.level == logging.DEBUG:
            stdout_p = open(os.path.join(self.aux_dir, f'nl-{node_name}-start-agent-stdout.log'), 'w')
            stderr_p = open(os.path.join(self.aux_dir, f'nl-{node_name}-start-agent-stderr.log'), 'w')

        logging.debug('running agent process via slurm with args: %s', ' '.join(
            [shutil.which('srun')] + self.node_local_agent_cmd + args))

        ExEvent.agent_launch_begin({'node': node_name, 'type': 'slurm'}).publish()
        return await asyncio.create_subprocess_exec(shutil.which('srun'), *slurm_args,
                                                    *self.node_local_agent_cmd, *args, stdout=stdout_p, stderr=stderr_p)

    async def _init_input_iface(self, proto='tcp', ip_addr='0.0.0.0', port_min=MIN_PORT_RANGE,
                                port_max=MAX_PORT_RANGE):
        """Initialize input interface.
        The ZMQ socket is created and binded for incoming notifications (from node agents).

        Args:
            proto - optional protocol for ZMQ socket
            ip_addr - optional local IP address
            port_min - optional minimum range port number
            port_max - optional maximum range port number
        """
        self.zmq_ctx = Context.instance()
        self.in_socket = self.zmq_ctx.socket(zmq.REP) #pylint: disable=maybe-no-member

        addr = f'{proto}://{ip_addr}'
        port = self.in_socket.bind_to_random_port(addr, min_port=port_min, max_port=port_max)
        address = f'{addr}:{port}'

        self.local_address = address
        self.local_export_address = f'{proto}://{socket.gethostbyname(socket.gethostname())}:{port}'

        logging.debug(f'local address accessible by other machines: {self.local_export_address}')

        self.iface_task = asyncio.ensure_future(self._input_iface())

    async def _input_iface(self):
        """The handler for input interface.
        The incoming notifications are processed and launcher state is modified.
        """
        while True:
            try:
                msg = await self.in_socket.recv_json()
                if msg is None:
                    logging.info('launcher input iface task got end of messages - finishing')
                    return
            except asyncio.CancelledError:
                logging.info('launcher input iface task got cancel error')
                raise

            logging.debug(f'received message: {msg}')

            if msg.get('status', None) == 'READY':
                try:
                    if not all(('agent_id' in msg, 'local_address' in msg)):
                        raise Exception('missing identifier/local address in ready message')
                except Exception as exc:
                    logging.error(f'error: {str(exc)}')
                    await self.in_socket.send_json({'status': 'ERROR', 'message': str(exc)})
                    continue
                else:
                    await self.in_socket.send_json({'status': 'CONFIRMED'})

                ExEvent.agent_launch_end({'node': msg['agent_id']}).publish()

                self.nodes[msg['agent_id']] = {'registered_at': datetime.now(), 'address': msg['local_address']}
                logging.debug('registered at (%s) agent (%s) listening at (%s)',
                              self.nodes[msg['agent_id']]['registered_at'],
                              msg['agent_id'], self.nodes[msg['agent_id']]['address'])
            elif msg.get('status', None) in ['APP_FINISHED', 'APP_FAILED']:
                await self.in_socket.send_json({'status': 'CONFIRMED'})

                appid = msg.get('appid', 'UNKNOWN')
                status = msg.get('status')

                ExEvent.agent_app_finish({'appid': appid, 'status': status}).publish()

                logging.debug(f'received notification with application {appid} finish {msg}')

                if appid in self.jobs_cb:
                    try:
#                        if self.jobs_cb[appid].get('args'):
#                            args = (self.jobs_cb[appid]['args'], msg)
#                        else:
#                            args = (msg)
                        args = (self.jobs_cb[appid]['args'], status, msg) \
                            if self.jobs_cb[appid].get('args') else (status, msg)
#                        logging.debug(f'calling application finish callback with object {self.jobs_cb[appid]["f"]}'
#                                      f' and args {args}')
                        self.jobs_cb[appid]['f'](*args)
                    except Exception as exc:
                        logging.exception(f'failed to call application finish callback: {str(exc)}')

                    del self.jobs_cb[appid]
                elif self.jobs_def_cb:
                    logging.debug(f'calling application default finish callback with object {self.jobs_def_cb["f"]}')
                    try:
                        args = (*self.jobs_def_cb['args'], msg) if self.jobs_def_cb['args'] else (msg)
                        self.jobs_def_cb['f'](args)
                    except Exception as exc:
                        logging.error(f'failed to call application default finish callback: {str(exc)}')
                else:
                    logging.debug('NOT calling application finish callback')
            elif msg.get('status', None) == 'FINISHING':
                await self.in_socket.send_json({'status': 'CONFIRMED'})

                logging.debug(f'received notification with node agent finish: {str(msg)}')

                if 'agent_id' in msg and msg['agent_id'] in self.nodes:
                    logging.debug(f'removing node agent {msg["agent_id"]}')
                    del self.nodes[msg['agent_id']]
                else:
                    logging.error(f'node agent {msg.get("agent_id", "UNKNOWN")} notifing FINISH not known')
            else:
                await self.in_socket.send_json({'status': 'ERROR'})
