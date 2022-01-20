from datetime import datetime
import logging
import json


class IQExecutor:

    # maximum time (in seconds) from the last heart beat signal to treat executor as active
    ACTIVE_MAX_HEARTBEAT_TRESHOLD = 120

    def __init__(self, name, address, resources):
        """This class stores information about registered executor.

        Attributes:
            name (str): unique executor name
            address (str): executor network address
            resources (ResourceStats): available resources statistics
            last_heart_beat (DateTime): last heart beat
            allocations (dict()): received job allocations
        """
        self.id = id
        self.name = name
        self.address = address
        self.resources = self._validate_resources(resources)

        self.last_heart_beat = datetime.now()
        self.allocations = {}

    def _validate_resources(self, resources):
        if 'nodes' not in resources:
            raise TypeError('missing nodes definition in executor resources description')

        if not isinstance(resources.get('nodes'), list):
            raise TypeError('nodes definition in executor resources description must be an list')

        total_cpus = 0
        node_max_cpus = 0
        for node in resources.get('nodes'):
            if 'name' not in node:
                raise TypeError('missing node name in executor resources description')

            if 'total_cores' not in node:
                raise TypeError(f'missing total_cores in {node.get("name")} node description')

            node_max_cpus = max(node_max_cpus, node.get('total_cores'))
            total_cpus += node.get('total_cores')

        resources['node_max_cpus'] = node_max_cpus
        resources['total_cpus'] = total_cpus
        resources['total_nodes'] = len(resources.get('nodes'))

        logging.info(f'{self.name} executor resources (after validation): {json.dumps(resources, indent=2)}')
        return resources

    def heart_beat(self):
        """Signal executor heart beat."""
        self.last_heart_beat = datetime.now()

    def last_heart_beat_seconds(self):
        """Return number of seconds from the last heart beat.

        Returns:
            float: number of seconds from the last heart beat
        """
        return (datetime.now() - self.last_heart_beat).total_seconds()

    def is_active(self):
        """Check if last heart beat signal has been sent in ACTIVE_MAX_HEARTBEAT_TRESHOLD seconds."""
        return self.last_heart_beat_seconds() < IQExecutor.ACTIVE_MAX_HEARTBEAT_TRESHOLD

    def get_executor_socket(self):
        zmq_socket = zmq_ctx.socket(zmq.REQ)  # pylint: disable=maybe-no-member
        zmq_socket.connect(self.address)
        return

    def append_allocation(self, allocation):
        """Append allocation to the list of executor's current allocations."""
        #TODO
        pass

    def remove_allocation(self, allocation):
        """Remove allocation from the list of executor's current allocations."""
        #TODO
        pass
