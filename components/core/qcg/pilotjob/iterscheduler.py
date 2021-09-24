""" The definition of iteration resources schedulers.

The role of iteration resources scheduler is to based on single iteration resource requirements described as a minimum
number of resources and number of available resources in allocation, assign exact number of resources in order to
optimize resources usage. Therefore the job's resource requirements do not have to be changed for different allocations.
The resource requirements can apply to both: number of cores and number of nodes specifications.
"""
import logging
import math

from qcg.pilotjob.errors import InvalidRequest


_logger = logging.getLogger(__name__)


class IterScheduler:
    """Iteration resources schedulers utility class."""

    @classmethod
    def get_scheduler(cls, name):
        """Return scheduler with given name.

        Args:
            name (str): scheduler name

        Returns:
            scheduler instance with given name, or if no such scheduler is available the default implementation
              (DefaultScheduler)
        """
        return __SCHEDULERS__.get(name.lower(), DefaultScheduler)

    @staticmethod
    def get_exact_iter_plan(iter_plan, exact):
        """Replace range style resource requirements with the exact one.

        Remove ``max``, ``min``, ``scheduler`` from the resource requirements and place as exact value the given value

        Args:
            iter_plan (joblist.ResourceSize): the instance of resource requirements to modify
            exact (int): the exact value for resource requirements

        Returns:
            the cloned and modified version of resource requirements
        """
        target = iter_plan.copy()

        if 'min' in target:
            del target['min']

        if 'max' in target:
            del target['max']

        if 'scheduler' in target:
            del target['scheduler']

        target['exact'] = exact
        return target


class MaximumIters:
    """The iteration resource scheduler for maximizing resource usage.

    The ``maximum-iters`` iteration resource scheduler is trying to launch as many iterations in the same time on all
    available resources. In case where number of iterations exceeds the number of available resources, the
    'maximum-iters' schedulers splits iterations into 'steps' minimizing this number, and allocates as many resources
    as possible for each iteration inside 'step'.
    """
    SCHED_NAME = 'maximum-iters'

    def __init__(self, job_resources, iterations, avail_resources, **params): #pylint: disable=unused-argument
        """Create ``maximum-iters`` iteration resource scheduler instance.

        Args:
            job_resources (joblist.ResourceSize): job's resource requirements
            iterations (int): number of iterations
            avail_resources (int): number of available resources
            params (dict): additional scheduler parameters
        """
        self.job_resources = job_resources
        self.iterations = iterations
        self.avail_resources = avail_resources

    def generate(self):
        """Generate exact job's resource requirements for next iteration.

        Yields:
            exact resource requirements for following iterations

        Raises:
            InvalidRequest: when parameter ``max`` is used in resource description
        """
        _logger.debug("iteration scheduler '%s' algorithm called", MaximumIters.SCHED_NAME)

        if 'max' in self.job_resources:
            raise InvalidRequest(
                'Wrong submit request - split-into directive mixed with max directive')

        pmin = 1
        if 'min' in self.job_resources:
            pmin = self.job_resources['min']

        if self.iterations * pmin <= self.avail_resources:
            # a single round
            _logger.debug("iterations in single round to schedule: %s, available resources: %s, "
                          "minimum iteration resources: %s", self.iterations, self.avail_resources, pmin)

            avail_resources = self.avail_resources
            for iteration in range(self.iterations):
                # assign part of round_resources to the iteration_in_round
                iteration_resources = math.floor(avail_resources / (self.iterations - iteration))
                avail_resources -= iteration_resources

                _logger.debug("iteration: %s/%s, iteration_resources: %s, rest avail_resources: %s",
                              iteration, self.iterations, iteration_resources, avail_resources)

                yield IterScheduler.get_exact_iter_plan(self.job_resources.copy(), iteration_resources)
        else:
            # more than one round

            # minimum number of needed rounds
            rounds = math.ceil((self.iterations / math.floor(float(self.avail_resources) / pmin)))
            iterations_to_schedule = self.iterations

            _logger.debug("iterations to schedule: %s, rounds: %s, resources: %s, minimum iteration "
                          "resources: %s", iterations_to_schedule, rounds, self.avail_resources, pmin)

            while iterations_to_schedule > 0:
                for ex_round in range(rounds):
                    iterations_in_round = math.ceil(iterations_to_schedule / (rounds - ex_round))
                    round_resources = self.avail_resources

                    _logger.debug("round: %s/%s, iterations_in_round: %s", ex_round, rounds, iterations_in_round)

                    for iteration_in_round in range(iterations_in_round):
                        # assign part of round_resources to the iteration_in_round
                        iteration_resources = math.floor(round_resources / (iterations_in_round - iteration_in_round))
                        round_resources -= iteration_resources

                        _logger.debug("round: %s/%s, iteration_in_round: %s/%s, iteration_resources: %s, rest "
                                      "round_resources: %s", ex_round, rounds, iteration_in_round, iterations_in_round,
                                      iteration_resources, round_resources)

                        yield IterScheduler.get_exact_iter_plan(self.job_resources.copy(), iteration_resources)

                    iterations_to_schedule -= iterations_in_round
                    _logger.debug("end of round: %s/%s, iterations_to_schedule: %s", ex_round, rounds,
                                  iterations_to_schedule)


class SplitInto:
    """The iteration resource scheduler for partitioning available resources.

    This simple iteration resource scheduler splits all available resources in given partitions, and each iteration
    will be executed inside whole single partition.
    """
    SCHED_NAME = 'split-into'

    def __init__(self, job_resources, iterations, avail_resources, **params):
        """Create ``split-into`` iteration resource scheduler instance.

        The number of partitions is taken as value of ``parts`` key of ``params`` dictionary if exists, and number of
        iterations in other case.

        Args:
            job_resources (joblist.ResourceSize): job's resource requirements
            iterations (int): number of iterations
            avail_resources (int): number of available resources
            params (dict): additional scheduler parameters
        """
        self.job_resources = job_resources
        self.iterations = iterations
        self.avail_resources = avail_resources

        if 'parts' in params:
            self.split_into = int(params['parts'])
        else:
            self.split_into = self.iterations

    def generate(self):
        """Generate exact job's resource requirements for next iteration.

        Yields:
            exact resource requirements for following iterations

        Raises:
            InvalidRequest: when parameter ``max`` is used in resource description
        """
        _logger.debug("iteration scheduler '%s' algorithm called", SplitInto.SCHED_NAME)

        if 'max' in self.job_resources:
            raise InvalidRequest(
                'Wrong submit request - split-into directive mixed with max directive')

        split_part = int(math.floor(self.avail_resources / self.split_into))
        if split_part <= 0:
            raise InvalidRequest('Wrong submit request - split-into resolved to zero')

        _logger.debug("split-into algorithm of %s self.iterations with %s split-into on %s self.avail_resources "
                      "finished with %s splits", self.iterations, self.split_into, self.avail_resources, split_part)

        for _ in range(0, self.iterations):
            yield IterScheduler.get_exact_iter_plan(self.job_resources.copy(), split_part)


DefaultScheduler = MaximumIters

__SCHEDULERS__ = {
    MaximumIters.SCHED_NAME.lower(): MaximumIters,
    SplitInto.SCHED_NAME.lower(): SplitInto,
}
