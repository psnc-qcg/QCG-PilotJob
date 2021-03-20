import logging

from qcg.pilotjob.allocation import Allocation
from qcg.pilotjob.errors import InternalError, InvalidResourceSpec, NotSufficientResources
from qcg.pilotjob.joblist import JobResources
from qcg.pilotjob.scheduleralgo import SchedulerAlgorithm


_logger = logging.getLogger(__name__)


def next_idx(l, idx):
    return (idx + 1) % len(l)

def cycle_list(l, pos=0):
    l_len = len(l)

    for i in range(l_len):
        idx = (pos + i) % l_len
        yield (idx, l[idx])


class ScatterSchedulerAlgorithm(SchedulerAlgorithm):
    """Scheduling algorithm.

    Attributes:
        resources (Resources): available resources
    """

    def __init__(self, resources=None):
        """Initialize scheduling algorithm

        Args:
            resources (Resources, optional): available resources
        """
        super(ScatterSchedulerAlgorithm, self).__init__(resources)
        
        self.next_allocated_node_idx = 0


    def allocate_cores(self, min_cores, max_cores=None):
        """Create allocation with maximum number of cores from given range.

        The cores will be allocated in a linear method but starting from next node
        to the previously allocated, so in case of single core allocations each
        next job will be allocated on next node.

        Args:
            min_cores (int): minimum requested number of cores
            max_cores (int): maximum requested number of cores

        Returns:
            Allocation: created allocation
            None: not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
            InvalidResourceSpec: when the min_cores < 0 or min_cores > max_cores
        """
        self._check_resources()

        if max_cores is None:
            max_cores = min_cores

        if min_cores <= 0 or min_cores > max_cores:
            raise InvalidResourceSpec()

        if self.resources.total_cores < min_cores:
            raise NotSufficientResources()

        if self.resources.free_cores < min_cores:
            return None

        allocation = Allocation()
        allocated_cores = 0

        for node_idx, node in cycle_list(self.resources.nodes, self.next_allocated_node_idx):
            node_alloc = node.allocate_max(max_cores - allocated_cores)

            if node_alloc:
                allocation.add_node(node_alloc)

                allocated_cores += node_alloc.ncores

                if allocated_cores == max_cores:
                    self.next_allocated_node_idx = next_idx(self.resources.nodes, node_idx)
                    break

        # this should never happen
        if allocated_cores < min_cores or allocated_cores > max_cores:
            allocation.release()
            raise NotSufficientResources()

        return allocation

    def _allocate_entire_nodes(self, min_nodes, max_nodes, crs):
        """Create allocation with specified number of entire nodes (will all available cores).

        Args:
            min_nodes (int) - minimum number of nodes
            max_nodes (int) - maximum number of nodes
            crs (dict(CRType,int)) - consumable resources requirements per node

        Returns:
            Allocation: created allocation or None if not enough free resources
        """
        allocation = Allocation()

        for node_idx, node in cycle_list(self.reosurces.nodes, self.next_allocated_node_idx):
            if node.used == 0:
                node_alloc = node.allocate_exact(node.total, crs)
                if node_alloc.ncores == node.total:
                    allocation.add_node(node_alloc)

                    if len(allocation.nodes) == max_nodes:
                        self.next_allocated_node_idx = next_idx(self.resources.nodes, node_idx)
                        break
                else:
                    node_alloc.release()

        if len(allocation.nodes) >= min_nodes:
            return allocation

        allocation.release()
        return None

    def _allocate_cores_on_nodes(self, min_nodes, max_nodes, min_cores, max_cores, crs):
        """Create allocation with specified number of nodes, where on each node should be
        given number of allocated cores.

        Args:
            min_nodes (int): minimum number of nodes
            max_nodes (int): maximum number of nodes
            min_cores (int): minimum number of cores to allocate on each node
            max_cores (int): maximum number of cores to allocate on each node
            crs (dict(CRType, int)) - consumable resource requirements per node

        Returns:
            Allocation: created allocation
            None: not enough free resources
        """
        allocation = Allocation()

        for node_idx, node in cycle_list(self.reosurces.nodes, self.next_allocated_node_idx):
            if node.free >= min_cores:
                node_alloc = node.allocate_max(max_cores, crs)

                if node_alloc:
                    if node_alloc.ncores >= min_cores:
                        allocation.add_node(node_alloc)

                        if len(allocation.nodes) == max_nodes:
                            self.next_allocated_node_idx = next_idx(self.resources.nodes, node_idx)
                            break
                    else:
                        node_alloc.release()

        if len(allocation.nodes) >= min_nodes:
            _logger.debug("allocation contains %d nodes which meets requirements (%d min)", len(allocation.nodes),
                         min_nodes)
            return allocation

        _logger.debug("allocation contains %d nodes which doesn't meets requirements (%d min)", len(allocation.nodes),
                     min_nodes)
        allocation.release()
        return None
