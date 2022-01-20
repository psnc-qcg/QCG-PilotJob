import logging
import asyncio
from qcg.pilotjob.common.errors import NotSufficientResources, InvalidResourceSpec
from qcg.pilotjob.executor.events import ExEvent

_logger = logging.getLogger(__name__)


class SchedulingJob:

    def __init__(self, job_iterations):
        self.job_iterations = job_iterations
        self.iterations = job_iterations.iterations

        self.minimum_required_cores = self.job_iterations.description.resources.get_min_num_cores()

    def id(self):
        return self.job_iterations.id

    def name(self):
        return self.job_iterations.name

    def resources(self):
        return self.job_iterations.description.resources

    def ready_iterations(self):
        # make a copy so the poping elements while iterating could work
        iterations = self.iterations.copy()
        for iteration in iterations:
            yield iteration

    def pop_iteration(self, iteration):
        if self.iterations:
            self.iterations.remove(iteration)

    def has_more_iterations(self):
        print(f'job {self.id()} ({self.name()}) contains total #{len(self.iterations)} iterations')
        return bool(self.iterations)


class ScheduleQueue:

    """Operate scheduling queue.
    Enqueue jobs for execution, find resources and send jobs to execute on specific allocation.

    Args:
        scheduler (Scheduler): scheduler for allocating resources
        execute_job (def): callback used to execute job iteration on specific allocation, passing arguments:
            scheduling_iteration, allocation
        failed_job (def): callback used to mark job iteration as failed (exceeds available resources), passing arguments:
            scheduling_iteration, error message
    """
    def __init__(self, scheduler, execute_job, failed_job):
        self.scheduler = scheduler
        self.execute_job = execute_job
        self.failed_job = failed_job

        self.schedule_queue = list()

        self.queue_lock = asyncio.Lock()

        self.waiting_cores = 0

    async def enqueue(self, jobs):
        """Enqueue jobs in scheduling queue.

        Args:
            jobs (iterable(JobIterations)): list of jobs to enqueue
        """
        async with self.queue_lock:
            for job in jobs:
                s_job = SchedulingJob(job)
                self.schedule_queue.append(s_job)
                self.waiting_cores += s_job.minimum_required_cores * len(s_job.iterations)

    async def schedule_loop(self):
        """Perform schedule loop.
        Until there are available resources, check if enqueued jobs can be allocated on free resources.
        This method should be called at every event:
            * new submited job
            * one of job finished
        """
        async with self.queue_lock:
            ExEvent.schedule_loop_begin({'jobs': len(self.schedule_queue)}).publish()

            logging.info(f'performing schedule loop with {len(self.schedule_queue)} jobs in queue ...')
            next_step_schedule_queue = list()

            for idx, scheduling_job in enumerate(self.schedule_queue):
                if not self.scheduler.resources.free_cores:
                    next_step_schedule_queue.extend(self.schedule_queue[idx:])
                    break

                min_res_cores = scheduling_job.minimum_required_cores

                if min_res_cores is not None and min_res_cores > self.scheduler.resources.total_cores:
                    _logger.debug(f'minimum # of cores {min_res_cores} for job {scheduling_job.name()} exceeds # of ' \
                                  f'total cores self.scheduler.resources.free_cores)')
                    _logger.warning(f'Job {scheduling_job.name()} scheduling failed - exceed available cores')
                    # remove all iterations
                    ready_iterations = scheduling_job.ready_iterations()
                    try:
                        job_iteration = next(ready_iterations)
                        scheduling_job.pop_iteration(job_iteration)
                        self.failed_job(scheduling_job.job_iterations, job_iteration, message='not enough available cores')
                    except StopIteration:
                        break

                    continue

                if min_res_cores is not None and min_res_cores > self.scheduler.resources.free_cores:
                    _logger.debug(f'minimum # of cores {min_res_cores} for job {scheduling_job.name()} exceeds # of ' \
                                  f'free cores {self.scheduler.resources.free_cores}')
                    break

                ready_iterations = scheduling_job.ready_iterations()
                while self.scheduler.resources.free_cores:
                    try:
                        job_iteration = next(ready_iterations)
                    except StopIteration:
                        break

                    # job is ready - try to find resources
                    _logger.debug(f'job {scheduling_job.name()}:{job_iteration} is ready')
                    try:
                        allocation = self.scheduler.allocate_job(scheduling_job.resources())
                        if allocation:
                            scheduling_job.pop_iteration(job_iteration)
                            _logger.debug(f'found resources for job {scheduling_job.name()}:{job_iteration}')

                            # allocation has been created - execute job
                            self.execute_job(scheduling_job.job_iterations, job_iteration, allocation)
                        else:
                            # missing resources
                            _logger.debug(f'missing resources for job {scheduling_job.name()}:{job_iteration}')
                    except (NotSufficientResources, InvalidResourceSpec) as exc:
                        # jobs will never schedule
                        _logger.warning(f'Job {scheduling_job.name()}:{job_iteration} scheduling failed - {str(exc)}')
                        scheduling_job.pop_iteration(job_iteration)
                        self.failed_job(scheduling_job.job_iterations, job_iteration, message=str(exc))

                if scheduling_job.has_more_iterations():
                    _logger.debug(f'Job {scheduling_job.name()} preserved in scheduling queue')
                    next_step_schedule_queue.append(scheduling_job)
                else:
                    _logger.debug(f'Job {scheduling_job.name()} removed from scheduling queue')

            self.schedule_queue = next_step_schedule_queue
            self.waiting_cores = 0
            for s_job in next_step_schedule_queue:
                self.waiting_cores += s_job.minimum_required_cores * len(s_job.iterations)

            ExEvent.schedule_loop_end({'jobs': len(self.schedule_queue)}).publish()
