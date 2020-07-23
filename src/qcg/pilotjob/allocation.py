
class CRAllocation:
    """Allocation of consumable resources

    Attributes:
        crtype (CRType): type of CR
        count (int): amount of allocated CR
    """

    def __init__(self, crtype, count):
        self.crtype = crtype
        self.count = count


class CRBindAllocation:
    """Allocation of bindable consumable resources

    Attributes:
        crtype (CRType): type of CR
        instances (list()): instances of allocated CR
    """

    def __init__(self, crtype, instances):
        self.crtype = crtype
        self.instances = instances

    @property
    def count(self):
        """int: number of instances of allocated CR"""
        return len(self.instances)


class NodeAllocation:
    """Resource allocation on a single node, contains information about a node,
    along with the allocated cores and consumable resources.

    Attributes:
        _node (Node): a node definition
        _cores (list(str)): allocated cores
        _crs (dict(CRType,CRAllocation|CRBindAllocation)): allocated crs
    """

    def __init__(self, node, cores, crs):
        assert node
        assert len(cores) > 0

        self._node = node
        self._cores = cores
        self._crs = crs

    def release(self):
        """Release resources allocated in this node allocation."""
        if self._node:
            self._node.release(self)
            self._node = None
            self._cores = None
            self._crs = None

    @property
    def cores(self):
        """list(str): allocated cores on a node."""
        return self._cores

    @property
    def ncores(self):
        """int: number of allocated cores on a node."""
        return len(self._cores)

    @property
    def crs(self):
        """dict((CRType, CRAllocation|CRBindAllocation)): allocated crs on a node."""
        return self._crs

    @property
    def node(self):
        """Node: node information."""
        return self._node

    def __str__(self):
        """Return a human readable description.

        Returns:
            str: a human readable description
        """
        return "{} @ {}".format(str(self._cores), self._node.name)


class Allocation:
    """
    Resource allocation splited (possible) among many nodes.

    Attributes:
        _nodes (NodeAllocation[]): list of a single node allocation
        _cores (int): total number of cores on all nodes
    """

    def __init__(self):
        """Initialize allocation."""
        self._nodes = []
        self._cores = 0

    def add_node(self, node_allocation):
        """Add a node allocation.

        Args:
            node_allocation (NodeAllocation): description of an allocation on a single node
        """
        assert node_allocation

        self._nodes.append(node_allocation)
        self._cores += node_allocation.ncores

    def release(self):
        """Release allocated resources.
        Release resources allocated on all nodes in allocation.

        Raises:
            InvalidResourceSpec: when number of cores to release on a node is greater
              than number of used cores.
        """
        for node in self._nodes:
            node.release()

        self._nodes = None
        self._cores = 0

    @property
    def cores(self):
        """Return total number of cores of an allocation

        Returns:
            int: number of cores
        """
        return self._cores

    @property
    def nodes(self):
        """Return a list of node allocations

        Returns:
            list(NodeAllocation): list of node allocations
        """
        return self._nodes

    def __str__(self):
        """Return a human readable string

        Returns:
            str: human readable string
        """
        header = "{} cores @ {} nodes\n".format(self._cores, len(self._nodes))
        return header + '\n'.join([str(node) for node in self._nodes])

    def description(self):
        """Return a single line description of allocation

        Returns:
            str: a single line description of allocation
        """
        return ','.join(["{}[{}]".format(node.node.name, ':'.join(str(e) for e in list(node.cores)))
                         for node in self._nodes])
