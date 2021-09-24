import multiprocessing as mp
import socket

from qcg.pilotjob.resources import Node, Resources, ResourcesType
from qcg.pilotjob.config import Config


def parse_local_resources(config):
    """Parse resources passed in configuration.

    The user can specify in configuration "virtual" resources such as number of nodes and cores.

    Args:
        config (dict): QCG-PilotJob configuration

    Returns:
        Resources: available resources

    Raises:
        ValueError: in case of missing node configuration or wrong number of cores configuration
    """
    nodes_conf = Config.EXECUTION_NODES.get(config)

    if nodes_conf:
        nodes = []
        nodes_list = nodes_conf.split(',')
        for nidx, node in enumerate(nodes_list):
            nname, _, ncores = node.rpartition(':')
            nname = nname or 'n{}'.format(nidx)

            if not ncores or int(ncores) < 1:
                raise ValueError('number of cores for a node must not be less than 1')

            nodes.append(Node(nname, int(ncores), 0))
    else:
        nodes = [Node(socket.gethostname(), mp.cpu_count(), 0)]

    if not nodes:
        raise ValueError('no node available')

    return Resources(ResourcesType.LOCAL, nodes)
