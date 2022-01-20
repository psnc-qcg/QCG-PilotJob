from qcg.pilotjob.common.errors import InvalidAllocation
from qcg.pilotjob.executor.scheduleralgo import SchedulerAlgorithm


class Scheduler:
    """Resource orchestration.

    Attributes:
        resources (Resources): available resources
        _scheduler_alg (SchedulerAlgorithm): scheduler algorithm
        _active_allocations (set(Allocation)): currently active allocations
    """

    def __init__(self, resources):
        """Initialize scheduler.

        Args:
            resources (Resources): available resources
        """
        self.resources = resources
        self._scheduler_alg = SchedulerAlgorithm(self.resources)
        self._active_allocations = set()

    def allocate_cores(self, min_cores, max_cores=None):
        """Create allocation with given number of cores.

        Args:
            min_cores (int): minimum requested number of cores
            max_cores (int): maximum requested number of cores, if None 'min_cores'
                             will mean also 'max_cores'

        Returns:
            Allocation: created allocation or None if not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
        """
        alloc = self._scheduler_alg.allocate_cores(min_cores, max_cores)

        if alloc is not None:
            self._active_allocations.add(alloc)

        return alloc

    def allocate_job(self, resources):
        """Create allocation for job with given resources.

        Args:
            resources (JobResources): job's resource requirements

        Returns:
            Allocation: created allocation or None if not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
            InvalidResourceSpec: when resource requirements are not valid
        """
        alloc = self._scheduler_alg.allocate_job(resources)

        if alloc is not None:
            self._active_allocations.add(alloc)

        return alloc

    def release_allocation(self, alloc):
        """Release resources assigned for the specificated allocation.

        Args:
            alloc (Allocation): allocation to release

        Raises:
            InvalidAllocation: when the allocation is not registered in the scheduler (it
              might be released earlier)
        """
        if alloc:
            if alloc not in self._active_allocations:
                raise InvalidAllocation()

            self._active_allocations.remove(alloc)
            alloc.release()
