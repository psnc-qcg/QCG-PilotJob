import zmq
import re
import socket
import logging

from zmq.asyncio import Context

from qcg.pilotjob.common.config import Var


class ZMQInterface:

    def __init__(self):
        """ZMQ interface for QCG-PilotJob InputQueue serivce."""

        self.zmq_ctx = None
        self.socket = None
        self.address = None
        self.external_address = None

    def start(self, config):
        """Setup interface. """
        self.zmq_ctx = Context.instance()
        self.address = config.get(Var.ZMQ_IFACE_ADDRESS)
        self.socket = self.zmq_ctx.socket(zmq.REP) #pylint: disable=maybe-no-member

        if re.search(r':[0-9]+$', self.address):
            self.socket.bind(self.address)
        else:
            self.socket.bind_to_random_port(self.address,
                                            min_port=int(config.get(Var.ZMQ_PORT_MIN_RANGE)),
                                            max_port=int(config.get(Var.ZMQ_PORT_MAX_RANGE)))

        self.real_address = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT))) #pylint: disable=maybe-no-member
        self.external_address = self.real_address
        if '//0.0.0.0:' in self.real_address:
            self.external_address = self.real_address.replace('//0.0.0.0:',
                                                              f'//{socket.gethostbyname(socket.gethostname())}:')

        logging.info(f'zmq interface address {self.real_address}')

    def stop(self):
        if self.socket:
            self.socket.close()

    async def receive(self):
        """Wait for incoming request.
        Each receive must be followed by reply.

        Returns:
            obj: incoming message as json object
        """
        return await self.socket.recv_json()

    async def reply(self, message):
        """Send reply message.
        The reply method should be called on every received message.

        Arguments:
            message (obj): a json object to send
        """
        await self.socket.send_json(message)
