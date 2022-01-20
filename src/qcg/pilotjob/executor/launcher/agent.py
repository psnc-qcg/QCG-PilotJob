import sys
import asyncio
import logging
import os
import socket
import json
import signal
from datetime import datetime
from os.path import join

import zmq
from zmq.asyncio import Context

from qcg.pilotjob.executor.launcher.processlauncher import ProcessLauncher
from qcg.pilotjob.common.allocation import AllocationDescription
from qcg.pilotjob.inputqueue.jobdesc import JobDescription
from qcg.pilotjob.executor.launcher.events import ExAgentEvent, init_event_publisher
from qcg.pilotjob.common.eventmonitor import EventPublisher, EventPublisherForwarder
import qcg.pilotjob.executor.launcher.events as events


class ExecutingJobIteration:

    """Description of job iteration to execute.

    Attributes:
        id (string) - job iteration identifier
        description (JobDescription) - job description
        attributes (dict) - job attributes
        iteration (str,int) - iteration index
        allocation (AllocationDescription) - resources allocation description
    """

    def __init__(self, id, description, attributes, iteration, allocation):
        self.id = id
        self.description = description
        self.attrs = attributes
        self.iteration = iteration
        self.allocation = allocation

        self.exit_code = -1
        self.message = None

        self.launcher = ProcessLauncher.get_launcher(self)

    async def launch(self):
        await self.launcher.prepare()
        await self.launcher.launch()


class Agent:
    """The node agent class.
    This class is responsible for launching jobs on local resources as well as submiting jobs
    to Slurm.

    Attributes:
        agent_id (str): agent identifier
        options (dict): agent options
        _finish (bool): true if agent should finish
        context (zmq.Context): ZMQ context
        in_socket (zmq.Socket): agent listening socket
        local_port (int): the local listening socket port
        local_address (str): the local listening socket address (proto://ip:port)
        remote_address (str): the launcher address
        local_export_address (str):
    """
    MIN_PORT_RANGE = 10000
    MAX_PORT_RANGE = 40000

    def __init__(self, agent_id, opts):
        """Initialize instance.

        Args:
            a_id - agent identifier
            opt - agent options
        """
        self.agent_id = agent_id
        self._finish = False

        self.options = opts

        # set default options
        self.options.setdefault('binding', False)

        logging.info(f'agent options: {self.options}')

        self.context = None
        self.in_socket = None
        self.local_port = None
        self.local_address = None
        self.remote_address = None
        self.local_export_address = None

        signal.signal(signal.SIGINT, self.sig_int_handler)

    def _clear(self):
        """Reset runtime settings. """
        self.context = None
        self.in_socket = None
        self.local_port = None
        self.local_address = None
        self.remote_address = None
        self.local_export_address = None

    async def agent(self, remote_address, ip_addr='0.0.0.0', proto='tcp', min_port=MIN_PORT_RANGE,
                    max_port=MAX_PORT_RANGE):
        """The agent handler method.
        The agent will listen on local incoming socket until it receive the EXIT command.
        At the start, the agent will sent to the launcher (on launcher's remote address) the READY message
        with the local incoming address.

        Args:
            remote_address (str): the launcher address where information about job finish will be sent
            ip_addr (str): the local IP where incoming socket will be bineded
            proto (str): the protocol of the incoming socket
            min_port (int): minimum port number to listen on
            max_port (int): maximum port number to listen on
        """
        self.remote_address = remote_address
        self.context = Context.instance()

        logging.debug(f'agent with id {self.agent_id} run to report to {self.remote_address}')

        # setup events
        if 'event_address' in self.options:
            logging.info(f'initializing event forwarder with remote address {self.options.get("event_address")}')
            init_event_publisher(EventPublisherForwarder(self.options.get('event_address')))
        else:
            init_event_publisher(EventPublisher())

        self.in_socket = self.context.socket(zmq.REP) #pylint: disable=maybe-no-member

        laddr = f'{proto}://{ip_addr}'

        self.local_port = self.in_socket.bind_to_random_port(laddr, min_port=min_port, max_port=max_port)
        self.local_address = f'{laddr}:{self.local_port}'
        self.local_export_address = f'{proto}://{socket.gethostbyname(socket.gethostname())}:{self.local_port}'

        logging.debug(f'agent with id {self.agent_id} listen at address {self.local_address}, export address '
                      f'({self.local_export_address})')

        try:
            await self._send_ready()
        except Exception:
            logging.error(f'failed to signal ready to manager: {sys.exc_info()}')
            self._cleanup()
            self._clear()
            raise

        while not self._finish:
            message = await self.in_socket.recv_json()

            cmd = message.get('cmd', 'UNKNOWN')

            await self.in_socket.send_json({'status': 'OK'})

            cmd = message.get('cmd', 'unknown').lower()
            if cmd == 'exit':
                self._cmd_exit(message)
            elif cmd == 'run':
                self._cmd_run(message)
            else:
                logging.error('unknown command received from launcher: %s', message)

        try:
            await self._send_finishing()
        except Exception as exc:
            logging.error('failed to signal shuting down: %s', str(exc))

        try:
            await events.event_publisher.stop()
        except Exception as exc:
            logging.error('failed to stop event handler: %s', str(exc))

        self._cleanup()
        self._clear()

    def _cleanup(self):
        """Close sockets."""
        if self.in_socket:
            self.in_socket.close()

    def sig_int_handler(self, sig, frame):
        logging.info('handling sig int signal - finishing')
        self._finish = True

    def _cmd_exit(self, message):
        """Handler of EXIT command.

        Args:
            message - message from the launcher
        """
        logging.debug('handling finish cmd with message (%s)', str(message))
        self._finish = True

    def _cmd_run(self, message):
        """Handler of RUN application command.

        Args:
            message - message with the following attributes:
                appid - application identifier
                args - the application arguments
        """
        logging.debug('running app %s with args %s ...', message.get('appid', 'UNKNOWN'), str(message.get('args', [])))

        asyncio.ensure_future(self._launch_iteration(
            message.get('appid', 'UNKNOWN'),
            description=message.get('description', []),
            attrs=message.get('attrs', None),
            iteration=message.get('iteration', None),
            allocation=message.get('allocation', None),
        ))

    async def _launch_iteration(self, appid, description, attrs, iteration, allocation):
        """Run application.

        Args:
            appid - application identifier
            description - job description
            attrs - job attributes
            iteration - the iteration index
            allocation - allocated resources
        """
        executing_it = ExecutingJobIteration(appid,
                                             JobDescription(**description),
                                             attrs,
                                             iteration,
                                             AllocationDescription(**allocation))

        starttime = datetime.now()

        try:
            await executing_it.launch()

            runtime = (datetime.now() - starttime).total_seconds()

            logging.info(f'process for job iteration {appid} finished with exit code {executing_it.exit_code} '
                         f'after {runtime} seconds')

            status_data = {
                'appid': appid,
                'agent_id': self.agent_id,
                'date': datetime.now().isoformat(),
                'status': 'APP_FINISHED',
                'ec': executing_it.exit_code,
                'runtime': runtime,
                'message': executing_it.message}
        except Exception as exc:
            logging.error(f'failed to launch process for job iteration {appid}: {str(exc)}')
            status_data = {
                'appid': appid,
                'agent_id': self.agent_id,
                'date': datetime.now().isoformat(),
                'status': 'APP_FAILED',
                'message': str(exc)}

        out_socket = self.context.socket(zmq.REQ) #pylint: disable=maybe-no-member
        out_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json(status_data)
            msg = await out_socket.recv_json()
            logging.debug(f'got confirmation for process finish: {str(msg)}')
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass

    async def _send_ready(self):
        """Send READY message to the launcher.
        The message will contain also the local listening address.
        """
        out_socket = self.context.socket(zmq.REQ) #pylint: disable=maybe-no-member
        out_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json({
                'status': 'READY',
                'date': datetime.now().isoformat(),
                'agent_id': self.agent_id,
                'local_address': self.local_export_address})

            msg = await out_socket.recv_json()

            logging.debug('received ready message confirmation: %s', str(msg))

            if not msg.get('status', 'UNKNOWN') == 'CONFIRMED':
                logging.error('agent %s not registered successfully in launcher: %s', self.agent_id, str(msg))
                raise Exception('not successfull registration in launcher: {}'.format(str(msg)))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass

    async def _send_finishing(self):
        """Send FINISHING message to the launcher.
        This message is the last message sent by the agent to the launcher before
        shuting down.
        """
        out_socket = self.context.socket(zmq.REQ) #pylint: disable=maybe-no-member
        out_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        logging.debug("sending finishing message")
        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json({
                'status': 'FINISHING',
                'date': datetime.now().isoformat(),
                'agent_id': self.agent_id,
                'local_address': self.local_address})
            logging.debug("finishing message sent, waiting for confirmation")
            msg = await out_socket.recv_json()

            logging.debug('received finishing message confirmation: %s', str(msg))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass


if __name__ == '__main__':
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print('error: wrong arguments\n\n\tagent {id} {remote_address} [options_in_json]\n\n')
        sys.exit(1)

    agent_id = sys.argv[1]
    raddress = sys.argv[2]
    options_arg = sys.argv[3] if len(sys.argv) > 3 else None

    print(f'options_arg: {options_arg}')
    options = {}
    if options_arg:
        try:
            options = json.loads(options_arg)
        except Exception as exc:
            print('failed to parse options: {}'.format(str(exc)))
            sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG,  # level=logging.INFO
        filename=join(options.get('auxDir', '.'), 'nl-agent-{}.log'.format(agent_id)),
        format='%(asctime)-15s: %(message)s')

    if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
        asyncio.set_event_loop(asyncio.new_event_loop())

    agent = Agent(agent_id, options)

    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(agent.agent(raddress)))
    asyncio.get_event_loop().close()

    logging.info('node agent %s exiting', agent_id)
    sys.exit(0)