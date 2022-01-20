import multiprocessing as mp
import socket

from qcg.pilotjob.common.resources import Node, Resources, ResourcesType
from qcg.pilotjob.common.config import Var


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
    nodes_conf = config.get(Var.EXECUTION_NODES)
    binding = False

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
        binding = True

    if not nodes:
        raise ValueError('no node available')

    return Resources(ResourcesType.LOCAL, nodes, binding)
