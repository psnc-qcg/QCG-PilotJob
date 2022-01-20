import asyncio
import logging

from .launcher.launcher import Launcher

from qcg.pilotjob.inputqueue.job import JobState
import qcg.pilotjob.executor.events as events


_logger = logging.getLogger(__name__)


class ExecutorJob:

    def __init__(self, job_iterations, iteration, allocation):
        """Information about executed job iteration.
        """
        self.job_iterations = job_iterations
        self.iteration = iteration
        self.allocation = allocation

        self.id = f'{self.job_iterations.id}:{iteration}'
        self.name = f'{self.job_iterations.name}:{iteration}'

        self.main_node = allocation.nodes[0].node.name

        self.exit_code = 0
        self.message = None

    def set_exit_code(self, code, message=None):
        self.exit_code = code
        self.message = message


class Executor:

    def __init__(self, config, resources, notify_job_started, notify_job_finished):
        """Manage execution of tasks.
        Store list of currently executed job iterations.

        Args:
            notify_job_started (def): callback that will be used to notify about job start
            notify_job_finished (def): callback that will be used to notify about job finish
        """
        self.notify_job_started = notify_job_started
        self.notify_job_finished = notify_job_finished
        self.jobs = dict()

        logging.info(f'available resources: {resources.to_json(indent=2)}')

        node_names = [node.name for node in resources.nodes]
        logging.info(f'executor event publisher: {events.event_publisher!r}')
        self.launcher = Launcher(resources.rtype, node_names, '.', '.',
                                 {'event_address': events.event_publisher.remote_iface_address})

    async def start_services(self):
        """Start agent instances on all nodes."""
        await self.launcher.start()

    async def stop_services(self):
        """Start agent instances on all nodes."""
        await self.launcher.stop()

    async def execute(self, job_iterations, iteration, allocation):
        """Execute job iteration with given allocation.

        Args:
            job_iterations (JobIterations): job iterations data
            iteration (str, int): iteration index
            allocation (Allocation): allocated resources
        """
        executor_job = ExecutorJob(job_iterations, iteration, allocation)

        self.jobs[executor_job.id] = executor_job

        self.notify_job_started(executor_job.job_iterations, executor_job.iteration, allocation)
        await self.launcher.submit(executor_job.main_node, executor_job, self._launched_iteration_finished,
                                   executor_job)

    def _launched_iteration_finished(self, executor_job, status, message):
        """Method executed by launcher when finish notification will be received.

        Args:
            executor_job (ExecutorJob): job iteration that finished
            message (dict()): finished notification message
        """
        logging.info(f'iteration finished callback with message {message}')
        executor_job.message = message.get('message')
        if status == 'APP_FAILED':
            executor_job.exit_code = 1
        else:
            executor_job.exit_code = message.get('ec', 0)

        self._iteration_stopped(executor_job)

    async def _start_iteration(self, executor_job):
        """Start execution of job iteration.
        Select proper launcher and use it to launch job iteration.

        Args:
            executor_job (ExecutorJob): executing job iteration data
        """
        try:
            executor_job.prepare()

            await executor_job.run()

            self._iteration_stopped(executor_job)
        except Exception as exc:
            _logger.exection(f'Failed to launch job {executor_job.id} ({executor_job.name})')
            executor_job.set_exit_code(-1, str(exc))
            self._iteration_stopped(executor_job)

    def _iteration_stopped(self, executor_job):
        """Job Iteration finished.

        Args:
            executor_iteration (ExecutorJob): job iteration that finished
        """
        del self.jobs[executor_job.id]
        self.notify_job_finished(executor_job.job_iterations, executor_job.iteration, executor_job.allocation,
                                 executor_job.exit_code, executor_job.message)
