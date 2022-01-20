from qcg.pilotjob.common.response import Response
from qcg.pilotjob.common.receiver import RequestHandler
from qcg.pilotjob.executor.events import ExEvent


class EXHandler(RequestHandler):
    """InputQueue interface handler"""

    # supported request names
    requests = [
        'stop',
        'stats',
        'ready_jobs',
    ]

    # prefix of handler functions
    handler_prefix = 'handle_'

    def __init__(self, exmanager):
        self.exmanager = exmanager

    def handle_stop(self, request):
        """Handle `stop` request.

        Args:
            request (dict): request data

        Raises:
        """
        ExEvent.handle_request({'name': 'stop'})
        return Response.error('not supported')

    def handle_stats(self, request):
        """Handle `stats` request.

        Args:
            request (dict): request data

        Raises:
        """
        ExEvent.handle_request({'name': 'stats'})
        return Response.error('not supported')

    def handle_ready_jobs(self, request):
        """Handle `ready_jobs` request.

        Args:
            request (dict): request data

        Raises:
        """
        ExEvent.handle_request({'name': 'ready_jobs'})
        pass
