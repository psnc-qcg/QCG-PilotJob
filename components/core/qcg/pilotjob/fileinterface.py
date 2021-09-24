import asyncio
import json
import logging
from os.path import exists

from qcg.pilotjob.errors import JobFileNotExist, IllegalJobDescription
from qcg.pilotjob.config import Config


_logger = logging.getLogger(__name__)


class FileInterface:
    """Interface for reading requests from file.

    Attributes:
        cnt (int): number of received requests so far
        path (str): path to the file from whom requests will be read
    """

    def __init__(self):
        self.cnt = 0
        self.path = None
        self.data = []

    @classmethod
    def name(cls):
        """Return interface' name.

        Returns:
            str: name of the interface
        """
        return "FILE"

    def setup(self, conf):
        """Setup interface.

        Open file and read all requests in JSON format. The loaded requests are returned in ``receive`` method.

        Args:
            conf: the QCG-PilotJob configuration

        Raises:
            JobFileNotExists: when file doens't exist
            IllegalJobDescription: when file doesn't contain the JSON format or the file doesn't contain the list.
        """
        self.data = []
        self.path = str(Config.FILE_PATH.get(conf))

        if not exists(self.path):
            raise JobFileNotExist(self.path)

        try:
            with open(self.path) as json_file:
                self.data = json.load(json_file)
        except Exception as exc:
            _logger.error("Fail to parse job description: %s", str(exc.args[0]))
            raise IllegalJobDescription("Wrong job description: {}".format(str(exc.args[0])))

        if not isinstance(self.data, list):
            raise IllegalJobDescription("Not an array of requests in json file")

    def close(self):
        """Close interface."""
        self.data = []

    async def receive(self):
        """Return next request.

        This method contains the artificial sleep, to get chance of other events in QCG-PilotJob to process. There could
        be a chance, that no job has been processed until all requests from file has been read.

        Returns: the next request or None if no more request is available.
        """
        if len(self.data) > 0:
            self.cnt = self.cnt + 1
            if self.cnt == 50:
                self.cnt = 0
                await asyncio.sleep(0.05)

            return self.data.pop(0)

        return None

    async def reply(self, reply_msg):
        """Reply response to the other part of this interface.

        Args:
            reply_msg (str): message to sent as reply

        Because file interface is used in batch mode, the response is ignored.
        """
        _logger.info("FileInterface reply message: %s", reply_msg)
