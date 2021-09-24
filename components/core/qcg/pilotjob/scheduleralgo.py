import logging

from qcg.pilotjob.allocation import Allocation
from qcg.pilotjob.errors import InternalError, InvalidResourceSpec, NotSufficientResources
from qcg.pilotjob.joblist import JobResources


_logger = logging.getLogger(__name__)


class SchedulerAlgorithm:
    """Scheduling algorithm.

    Attributes:
        resources (Resources): available resources
    """

    def __init__(self, resources=None):
        """Initialize scheduling algorithm

        Args:
            resources (Resources, optional): available resources
        """
        self.resources = resources

    def _check_resources(self):
        """Validate if the resources has been set.

        Raises:
            InternalError: when resources has not been set
        """
        if self.resources is None:
            raise InternalError("Missing resources in scheduler algorithm")

    def allocate_cores(self, min_cores, max_cores=None):
        """Create allocation with maximum number of cores from given range.

        The cores will be allocated in a linear method.

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
        for node in self.resources.nodes:
            if not node.available:
                _logger.info(f'node {node.name} not available')
            else:
                node_alloc = node.allocate_max(max_cores - allocated_cores)

                if node_alloc:
                    allocation.add_node(node_alloc)

                    allocated_cores += node_alloc.ncores

                    if allocated_cores == max_cores:
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

        for node in self.resources.nodes:
            if node.available:
                if node.used == 0:
                    node_alloc = node.allocate_exact(node.total, crs)
                    if node_alloc.ncores == node.total:
                        allocation.add_node(node_alloc)

                        if len(allocation.nodes) == max_nodes:
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

#        _logger.info("allocating ({},{}) nodes with ({},{}) cores".format(min_nodes, max_nodes, min_cores, max_cores))

        for node in self.resources.nodes:
            if node.available and node.free >= min_cores:
                node_alloc = node.allocate_max(max_cores, crs)

#                _logger.info("allocated {} cores on a single node".format(node_alloc.ncores if node_alloc else 0))

                if node_alloc:
                    if node_alloc.ncores >= min_cores:
                        allocation.add_node(node_alloc)

                        if len(allocation.nodes) == max_nodes:
                            break
                    else:
                        node_alloc.release()

        if len(allocation.nodes) >= min_nodes:
            _logger.info("allocation contains %d nodes which meets requirements (%d min)", len(allocation.nodes),
                         min_nodes)
            return allocation

        _logger.info("allocation contains %d nodes which doesn't meets requirements (%d min)", len(allocation.nodes),
                     min_nodes)
        allocation.release()
        return None

    def allocate_job(self, reqs):
        """Create allocation for job with given resource requirements.

        Args:
            reqs (JobResources): job's resource requirements

        Returns:
            Allocation: created allocation or None if not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
            InvalidResourceSpec: when resource requirements are not valid
        """
        self._check_resources()

        if reqs is None or not isinstance(reqs, JobResources):
            raise InvalidResourceSpec("Missing resource requirements")

        if not reqs.has_nodes and not reqs.has_cores:
            raise InvalidResourceSpec("No resources specified")

        min_nodes = 0
        if reqs.has_nodes:
            if reqs.nodes.is_exact():
                min_nodes = reqs.nodes.exact
            else:
                if reqs.nodes.min is None or reqs.nodes.max is None:
                    raise InvalidResourceSpec("Both node's range boundaries (min, max) must be defined")
                min_nodes = reqs.nodes.min

            if min_nodes > len(self.resources.nodes):
                raise NotSufficientResources(
                    "{} exceeds available number of nodes ({})".format(min_nodes, len(self.resources.nodes)))

        min_cores = 0
        total_cores = 0
        if reqs.has_cores:
            if reqs.cores.is_exact():
                min_cores = reqs.cores.exact
            else:
                if reqs.cores.min is None or reqs.cores.max is None:
                    raise InvalidResourceSpec("Both core's range boundaries (min, max) must be defined")
                min_cores = reqs.cores.min

            total_cores = min_nodes * min_cores

            if self.resources.total_cores < total_cores:
                raise NotSufficientResources(
                    "{} exceeds available number of cores ({})".format(total_cores, self.resources.total_cores))

            if self.resources.free_cores < total_cores:
                return None

        if min_nodes == 0 and min_cores == 0:
            raise InvalidResourceSpec("Missing node and cores specification")

        if reqs.has_crs and min_nodes == 0:
            raise InvalidResourceSpec("Number of nodes is required when CR are used")

        if reqs.has_crs:
            for cr, count in reqs.crs.items():
                if cr not in self.resources.max_crs or count > self.resources.max_crs[cr] or \
                        min_nodes * count > self.resources.total_crs[cr]:
                    raise NotSufficientResources("CR {} exceeds maximum available resources on node ({})".format(
                        cr.name, self.resources.max_crs[cr]))

        allocation = Allocation()

        if min_cores == 0:
            # allocate ('min_nodes', 'max_nodes') whole nodes
            max_nodes = min_nodes
            if not reqs.nodes.is_exact():
                max_nodes = reqs.nodes.max

            if (min_nodes == 0 and max_nodes == 0) or \
                    max_nodes < 1 or \
                    max_nodes < min_nodes:
                raise InvalidResourceSpec(
                    "Invalid nodes specification (min: {}, max: {})".format(min_nodes, max_nodes))

            allocation = self._allocate_entire_nodes(min_nodes, max_nodes, reqs.crs)
        else:
            max_cores = min_cores
            if not reqs.cores.is_exact():
                max_cores = reqs.cores.max

            if (min_cores == 0 and max_cores == 0) or \
                    max_cores < 1 or \
                    max_cores < min_cores:
                raise InvalidResourceSpec(
                    "Invalid cores specification (min: {}, max: {})".format(min_cores, max_cores))

            max_nodes = 1
            if reqs.has_nodes:
                if reqs.nodes.is_exact():
                    max_nodes = min_nodes
                else:
                    max_nodes = reqs.nodes.max

            if min_nodes == 0:
                # allocate ('min_cores', 'max_cores') on any number of nodes
                allocation = self.allocate_cores(min_cores, max_cores)
            else:
                # allocate ('min_cores', 'max_cores') on ('min_nodes', 'max_nodes') each
                allocation = self._allocate_cores_on_nodes(min_nodes, max_nodes, min_cores, max_cores, reqs.crs)

        return allocation
