import json
import logging
import os
import socket

import zmq
from zmq.asyncio import Context
from qcg.pilotjob.config import Config


_logger = logging.getLogger(__name__)


class ZMQInterface:
    """ZMQ interface for QCG-PilotJob.

    Attributes:
        zmq_ctx (Context): ZMQ context
        socket (socket): ZMQ socket
        address (str): address of ZMQ interface from configuration
        local_port (int): listen port number
        real_address (str): address obtained from ``getsockopt``
        external_address (str): address on external network interface (not on private ips)
    """

    @classmethod
    def name(cls):
        """Return interface name.

        Returns:
            str: interface name
        """
        return "ZMQ"

    def __init__(self):
        """Initialize ZMQ interface."""
        self.zmq_ctx = None
        self.socket = None
        self.address = None
        self.local_port = None
        self.real_address = None
        self.external_address = None

    def setup(self, conf):
        """Open ZMQ interface.

        If port number is not specified in QCG-PilotJob configuration, it is chosen randomly from configured range.
        """
#        zmq.asyncio.install()
        self.zmq_ctx = Context.instance()

        self.address = Config.ZMQ_IFACE_ADDRESS.get(conf)

        self.socket = self.zmq_ctx.socket(zmq.REP) #pylint: disable=maybe-no-member

        if Config.ZMQ_PORT.get(conf):
            self.socket.bind(self.address)
        else:
            self.local_port = self.socket.bind_to_random_port(self.address,
                                                              min_port=int(Config.ZMQ_PORT_MIN_RANGE.get(conf)),
                                                              max_port=int(Config.ZMQ_PORT_MAX_RANGE.get(conf)))

        self.real_address = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT)))#pylint: disable=maybe-no-member

        # the real address might contain the 0.0.0.0 IP address which means that it listens on all
        # interfaces, sadly this address is not valid for external services to communicate, so we
        # need to replace 0.0.0.0 with the real address IP
        self.external_address = self.real_address
        if '//0.0.0.0:' in self.real_address:
            self.external_address = self.real_address.replace('//0.0.0.0:', '//{}:'.format(
                socket.gethostbyname(socket.gethostname())))

        _logger.info('ZMQ interface configured (address %s) @ %s, external address @ %s', self.address,
                     self.real_address, self.external_address)

    def close(self):
        """Close ZMQ socket."""
        if self.socket:
            try:
                _logger.info('closing ZMQ socket')
                self.socket.close()
            except Exception:
                pass

    async def receive(self):
        """Wait for incoming request.

        Returns:
            dict: incoming request
        """
        _logger.info("ZMQ interface listening for requests with pid %d...", os.getpid())

        req = await self.socket.recv()

        _logger.info("ZMQ interface received request ...")

        return json.loads(bytes.decode(req))

    async def reply(self, reply_msg):
        """Sent reply.

        Args:
            reply_msg (str): message to sent
        """
        await self.socket.send(str.encode(reply_msg))
