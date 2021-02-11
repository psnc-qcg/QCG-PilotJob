import asyncio
import logging
import uuid
from datetime import datetime
import getpass
import socket
import os
import sys
import json
import traceback
from string import Template
import zmq

from qcg.pilotjob.errors import NotSufficientResources, InvalidResourceSpec
from qcg.pilotjob.errors import GovernorConnectionError, JobAlreadyExist
from qcg.pilotjob.executor import Executor
from qcg.pilotjob.joblist import JobList, JobState, JobResources
from qcg.pilotjob.scheduler import Scheduler
import qcg.pilotjob.profile
from qcg.pilotjob.config import Config
from qcg.pilotjob.parseres import get_resources
from qcg.pilotjob.request import ControlReq
from qcg.pilotjob.response import Response, ResponseCode
from qcg.pilotjob.errors import InvalidRequest
from qcg.pilotjob.iterscheduler import IterScheduler
from qcg.pilotjob.joblist import Job
from qcg.pilotjob.resume import StateTracker


_logger = logging.getLogger(__name__)


class SchedulingJob:
    """Data necessary for scheduling job.

    Dependencies.
    The SchedulingJob contains two set of dependencies - the common for all subjobs, and specific for each
    subjob (these dependencies are stored in SchedulingIteration). The execution of individual subjob might start
    only after all common dependencies has been meet, and those specific for each subjob.

    Attributes:
        manager (Manager): manager instance
        job (Job): job instance
        _is_feasible (bool): does the job has chance to meet dependencies
        _after_jobs (set): list of dependant jobs (without individual for each subjob) - common for all iterations
        _after_iteration_jobs (set): list of dependant individual subjobs for each subjob - specific for each iteration
        _iteration_sub_jobs (list): data of iteration subjobs
        _has_iterations (bool): flag for iterative jobs
        _total_iterations (int): total number of iterations
        _current_solved_iterations (int): number of currently solved iterations
        _res_cores_gen (Generator): generator for job iterations to compute # of cores for each iteration
        _res_nodes_gen (Generator): generator for job iterations to compute # of nodes for each iteration
        _min_res_cores (int): minimum number of cores required by single job iteration
    """

    # how many iterations should be resolved at each scheduling step
    ITERATIONS_SPLIT = 100

    def __init__(self, manager, job):
        """Initialize instance.

        Args:
            manager (Manager): the manager instance
            job (Job): job to schedule
        """
        self.manager = manager
        self.job = job

        # does the job chance to meet dependencies
        self._is_feasible = True

        # list of dependant jobs (without individual for each subjob) - common for all iterations
        self._after_jobs = set()

        # list of dependant individual subjobs for each subjob - specific for each iteration
        self._after_iteration_jobs = set()

        # data of iteration subjobs
        self._iteration_sub_jobs = []

        # flag for iterative jobs
        self._has_iterations = self.job.has_iterations

        # total number of iterations
        self._total_iterations = self.job.iteration.iterations() if self._has_iterations else 1

        # number of currently solved iterations
        self._current_solved_iterations = self._total_iterations - self.job.get_not_finished_iterations() \
            if self._has_iterations else 0

        # general dependencies
        if job.has_dependencies:
            for job_id in job.dependencies.after:
                if '${it}' in job_id:
                    self._after_iteration_jobs.add(job_id)
                else:
                    self._after_jobs.add(job_id)

            self.check_dependencies()

        # job resources
        # in case of job iterations, there are schedulers which generates # of cores/nodes specific for each iteration
        self._res_cores_gen = None
        jobres = self.job.resources
        if self._has_iterations and jobres.has_cores and jobres.cores.scheduler is not None:
            self._res_cores_gen = IterScheduler.get_scheduler(jobres.cores.scheduler['name'])(
                jobres.cores.to_dict(), self._total_iterations, self.manager.resources.total_cores,
                **jobres.cores.scheduler.get('params', {})).generate()
            _logger.debug('generated cores scheduler %s for job %s', jobres.cores.scheduler['name'], self.job.name)

        self._res_nodes_gen = None
        if self._has_iterations and jobres.has_nodes and jobres.nodes.scheduler is not None:
            self._res_nodes_gen = IterScheduler.get_scheduler(jobres.nodes.scheduler["name"])(
                jobres.nodes.to_dict(), self._total_iterations, self.manager.resources.total_nodes,
                **jobres.nodes.scheduler.get("params", {})).generate()
            _logger.debug('generated nodes scheduler %s for job %s', jobres.nodes.scheduler['name'], self.job.name)

        # compute minResCores
        self._min_res_cores = jobres.get_min_num_cores()
        _logger.debug('minimum # of cores for job %s is %d', self.job.name, self._min_res_cores)

        # generate only part of whole set of iterations
        self.setup_iterations()

    def check_dependencies(self):
        """Update dependency state.
        Check all dependent jobs and update job's ready (and possible feasible) status.
        """
        finished = set()

        # check job dependencies
        for job_id in self._after_jobs:
            job_name = job_id
            job_it = None

            if ":" in job_name:
                job_name, job_it = JobList.parse_jobname(job_name)

            dep_job = self.manager.job_list.get(job_name)
            if dep_job is None:
                _logger.warning("Dependency job \'%s\' not registered", job_id)
                self._is_feasible = False
                break

            dep_job_state = dep_job.state(job_it)
            if dep_job_state.is_finished():
                if dep_job.state() != JobState.SUCCEED:
                    self._is_feasible = False
                    break

                finished.add(job_id)

        self._after_jobs -= finished

        if self._iteration_sub_jobs and self._after_iteration_jobs:
            # check single iteration job dependencies
            to_remove = []
            for iteration_job in self._iteration_sub_jobs:
                if iteration_job is not None:
                    iteration_job.check_dependencies()

                    if not iteration_job.is_feasible:
                        self.manager.change_job_state(self.job, iteration=iteration_job.iteration,
                                                      state=JobState.OMITTED)
                        _logger.debug('iteration %s not feasible - removing', iteration_job.name)
                        to_remove.append(iteration_job)

            for iteration_job in to_remove:
                self._iteration_sub_jobs.remove(iteration_job)

        _logger.debug("#%d dependency (%s feasible) jobs after update of job %s",
                      len(self._after_jobs), str(self._is_feasible), self.job.name)

    def setup_iterations(self):
        """Resolve next part of iterations."""
        niters = min(self._total_iterations - self._current_solved_iterations, SchedulingJob.ITERATIONS_SPLIT)
        _logger.debug('solving %d iterations in job %s', niters, self.job.name)
        for iteration in range(self._current_solved_iterations, self._current_solved_iterations + niters):
            # prepare resources, dependencies
            subjob_iteration = iteration + self.job.iteration.start if self._has_iterations else None
            if self.job.state(subjob_iteration) != JobState.CANCELED:
                subjob_resources = self.job.resources

                if self._res_cores_gen or self._res_nodes_gen:
                    job_resources = subjob_resources.to_dict()

                    if self._res_cores_gen:
                        job_resources['numCores'] = next(self._res_cores_gen)

                    if self._res_nodes_gen:
                        job_resources['numNodes'] = next(self._res_nodes_gen)

                    subjob_resources = JobResources(**job_resources)

                subjob_after = None
                if self._after_iteration_jobs:
                    subjob_after = set()
                    for job_name in self._after_iteration_jobs:
                        subjob_after.add(job_name.replace('${it}', str(subjob_iteration)))

                sched_iter = SchedulingIteration(self, subjob_iteration, subjob_resources, subjob_after)
                self._iteration_sub_jobs.append(sched_iter)

        self._current_solved_iterations += niters
        _logger.debug('%d currently iterations solved in job %s', self._current_solved_iterations, self.job.name)

    def remove_iteration_index(self, iteration):
        """Remove iteration from solved iterations.

        Args:
            iteration (int): iteration index
        """
        try:
            it_pos = next(i for i, sched_it in enumerate(self._iteration_sub_jobs) if sched_it.iteration == iteration)
            _logger.info(f'found iteration {iteration} index {it_pos} in generated iterations - removing it')
            del self._iteration_sub_jobs[it_pos]
        except StopIteration:
            # ignore - iteration might not be generated yet
            pass

    @property
    def is_feasible(self):
        """bool: Check if job can be executed. Job that dependency will never be satisfied (dependent jobs failed)
            should never be run."""
        return self._is_feasible

    @property
    def is_ready(self):
        """bool: Check if job can be scheduled and executed. Jobs with not met dependencies can not be scheduled."""
        return len(self._after_jobs) == 0

    def get_ready_iteration(self, prev_iteration=None):
        """Return SchedulingIteration describing next ready iteration.

        Args:
            prev_iteration (int): if defined the next iteration should be after specified one

        Returns:
            SchedulingIteration: next ready iteration to allocate resources and execute or None - if none of iteration
                is ready to execute
        """
        if self.is_ready and not self.job.canceled:
            if self._has_iterations and not self._iteration_sub_jobs and \
                    self._current_solved_iterations < self._total_iterations:
                self.setup_iterations()

            start_pos = 0

            if prev_iteration:
                start_pos = self._iteration_sub_jobs.index(prev_iteration) + 1

            _logger.debug('job %s is ready, looking for next to (%s) iteration, start_pos set to %d',
                          self.job.name, str(prev_iteration), start_pos)

            repeats = 2
            while repeats > 0:
                for i in range(start_pos, len(self._iteration_sub_jobs)):
                    iteration_job = self._iteration_sub_jobs[i]
                    _logger.debug(f'found iteration in job {iteration_job} that is {iteration_job.is_ready}')
                    if iteration_job.is_ready:
                        return iteration_job

                if self._current_solved_iterations < self._total_iterations and repeats > 1:
                    start_pos = len(self._iteration_sub_jobs)
                    self.setup_iterations()
                    repeats = repeats - 1
                else:
                    break

        _logger.debug('no new iterations to schedule')
        return None

    @property
    def has_more_iterations(self):
        """bool: Check if job has more pending iterations, True - there are pending iterations, False - no more
            iterations, all iterations already scheduled"""
        return self._iteration_sub_jobs or self._current_solved_iterations < self._total_iterations

    def remove_iteration_job(self, iteration_job):
        """Called by the manager when iteration returned by the get_ready_iteration has been allocated resources and
        will be executed or it's resorce requirements exceedes available resources. This iteration should not be
        returned another time by the get_ready_iteration.
        """
        _logger.debug('iteration %s processed - removing from list', iteration_job.name)
        self._iteration_sub_jobs.remove(iteration_job)

    def get_minimum_require_cores(self):
        """The function returns a minimum number of cores that any iteration in the job requires. Such information
        can optimize scheduler.

        Returns:
            int: the minimum required number of cores by the iterations
        """
        return self._min_res_cores


class SchedulingIteration:
    """A single job iteration to schedule.

    Attributes:
        _scheduling_job (SchedulingJob): parent job
        _iteration (int): iteration index
        _resources (JobResources): resource requirements
        _after_subjobs (list): subjob dependencies
        _name (str): iteration name, if it's main job iteration the name is the same as job's name
        _is_feasible (bool): does the subjob has chance to execute
    """

    def __init__(self, scheduling_job, iteration, resources, after_subjobs):
        """Initialize instance.

        Args:
            scheduling_job (SchedulingJob): parent job
            iteration (int): iteration index
            resources (JobResources): resource requirements
            after_subjobs (list): iteration dependencies
        """
        # link to the parent job
        self._scheduling_job = scheduling_job

        # iteration identifier
        self._iteration = iteration

        # resource requirements
        self._resources = resources

        # individual subjob dependencies
        self._after_subjobs = after_subjobs

        # name of the subjob
        self._name = self._scheduling_job.job.name + (':{}'.format(self._iteration)
                                                      if self._iteration is not None else '')

        # does the subjob has chance to execute
        self._is_feasible = True

        if self._after_subjobs:
            self.check_dependencies()

    def check_dependencies(self):
        """Update individual subjob dependency state.
        Check all dependent jobs and update job's ready (and possible feasible) status.
        """
        if self._after_subjobs:
            finished = set()

            for job_id in self._after_subjobs:
                job_name = job_id
                job_it = None

                if ":" in job_name:
                    job_name, job_it = JobList.parse_jobname(job_name)

                dep_job = self._scheduling_job.manager.job_list.get(job_name)
                if dep_job is None:
                    _logger.warning("Dependency %s job not registered - %s not feasible", job_name, self.name)
                    self._is_feasible = False
                    break

                dep_job_state = dep_job.state(job_it)
                _logger.info(f'job {self.name} - checking dep it {job_it} of job {job_name} (in state {dep_job_state}')
                if dep_job_state.is_finished():
                    if dep_job_state != JobState.SUCCEED:
                        self._is_feasible = False
                        break

                    finished.add(job_id)

            self._after_subjobs -= finished

            _logger.debug("#%d dependency (%s feasible) jobs after update of job %s", len(self._after_subjobs),
                          str(self._is_feasible), self.name)

    @property
    def is_feasible(self):
        """bool: return the subjob 'health' status - does the subjob has a chance to execute."""
        return self._is_feasible

    @property
    def is_ready(self):
        """bool: Return the subjob readiness status - is it ready for execution (the all dependencies has been met),
            True - the job may be executed, False - not all dependent jobs has been finished """
        return not self._after_subjobs

    @property
    def name(self):
        """str: subjob name"""
        return self._name

    @property
    def job(self):
        """SchedulingJob: subjob parent job"""
        return self._scheduling_job.job

    @property
    def iteration(self):
        """int: iteration index"""
        return self._iteration

    @property
    def resources(self):
        """JobResources: resource requirements"""
        return self._resources


class JobStateCB:
    """Information about job status change callback.

    Attributes:
        callback (def): callback function
        args (list): callback function arugments
    """

    def __init__(self, callback, *args):
        """Initialize instance.

        Args:
            callback (def): callback function
            args (list): callback function arugments
        """
        self.callback = callback
        self.args = args


class DirectManager:
    """Manager of jobs to execution.
    The incoming jobs are scheduled and executed.

    Attributes:
        resources (Resources): available resources
        _executor (Executor): executor instance used to execute job iterations
        _scheduler (Scheduler): scheduler instance used to allocate resources for job iterations
        job_list (JobList): list of all submited jobs
        _schedule_queue (list(SchedulingJob)): list of currently scheduled jobs
        _job_states_cbs (dict): list of registered job status change callbacks
        zmq_address (str): address of ZMQ interface
        manager_id (str): manager instance identifier
        manager_tags (str): manager instance tags
        _parent_manager (str): address of governor manager interface
        stop_processing (bool): if set to True, no job status change will be registered
    """

    def __init__(self, tracer, config=None, parent_manager=None):
        """Initialize instance.

        Args:
            tracer (StateTracker): tracker object used to track new jobs and job status changes (resume mechanism)
            config (dict): QCG-PilotJob configuration
            parent_manager (str): address of the governor manager
        """
        conf = config or None
        self.tracer = tracer
        self.resources = get_resources(conf)

        if Config.SYSTEM_CORE.get(conf):
            self.resources.allocate_for_system()

        _logger.info('available resources: %s', self.resources)

        self._executor = Executor(self, conf, self.resources)
        self._scheduler = Scheduler(self.resources, conf)
        self.job_list = JobList()

        self._schedule_queue = []

        self._job_states_cbs = {}

        self.zmq_address = None

        self.manager_id = Config.MANAGER_ID.get(conf)
        self.manager_tags = Config.MANAGER_TAGS.get(conf)

        self._parent_manager = parent_manager

        self.stop_processing = False

        # used to track jobs that has been removed from schedule_queue but not
        # started (registered in executor._not_finished dict)
        self.queued_to_execute = 0


    async def setup_interfaces(self):
        """Initialize manager after all incoming interfaces has been started. """
        if self._parent_manager:
            try:
                _logger.info('registering in parent manager %s ...', self._parent_manager)
                await self.register_in_parent()
                self.register_notifier(self._notify_parent_with_job)
            except Exception:
                _logger.error('Failed to register manager in parent governor manager: %s', sys.exc_info()[0])
                raise
        else:
            _logger.info('no parent manager set')

    def set_zmq_address(self, zmq_address):
        """Set ZMQ address of input interface.

        Args:
            zmq_address (str): input address of listening ZMQ interface
        """
        self.zmq_address = zmq_address

    def get_handler(self):
        """Return request handler.

        Returns:
            request handler
        """
        return DirectManagerHandler(self)

    async def stop(self):
        """Stop all services.
        The executor is stoped.
        """
        if self._executor:
            await self._executor.stop()

    @property
    def is_all_jobs_finished(self):
        """bool: returns True if there are no jobs in scheduling queue and no jobs are executing"""
        return len(self._schedule_queue) == 0 and self._executor.is_all_jobs_finished() and self.queued_to_execute == 0

    @profile
    def _schedule_loop(self):
        """Do schedule loop.
        Get jobs from schedule queue, check if they have workflow dependency meet and if yes,
        try to create allocation. The allocated job's are sent to executor.
        """
        new_schedule_queue = []

        if self.stop_processing:
            return

        _logger.debug(f"scheduling loop with {len(self._schedule_queue)} jobs in queue")

        for idx, sched_job in enumerate(self._schedule_queue):
            if not self.resources.free_cores:
                new_schedule_queue.extend(self._schedule_queue[idx:])
                break

            min_res_cores = sched_job.get_minimum_require_cores()
            if min_res_cores is not None and min_res_cores > self.resources.free_cores:
                _logger.debug('minimum # of cores %d for job %s exceeds # of free cores %s', min_res_cores,
                              sched_job.job.name, self.resources.free_cores)
                DirectManager._append_to_schedule_queue(new_schedule_queue, sched_job)
                continue

            sched_job.check_dependencies()

            if not sched_job.is_feasible:
                # job will never be ready
                _logger.debug("job %s not feasible - omitting", sched_job.job.name)
                self.change_job_state(sched_job.job, iteration=None, state=JobState.OMITTED)
                sched_job.job.clear_queue_pos()
            else:
                prev_iteration = None
                while self.resources.free_cores:
                    if min_res_cores is not None and min_res_cores > self.resources.free_cores:
                        _logger.debug('minimum # of cores %d for job %s exceeds # of free cores %d', min_res_cores,
                                      sched_job.job.name, self.resources.free_cores)
                        break

                    job_iteration = sched_job.get_ready_iteration(prev_iteration)
                    if job_iteration:
                        # job is ready - try to find resources
                        _logger.debug("job %s is ready", job_iteration.name)
                        try:
                            allocation = self._scheduler.allocate_job(job_iteration.resources)
                            if allocation:
                                sched_job.remove_iteration_job(job_iteration)
                                prev_iteration = None

                                _logger.debug("found resources for job %s", job_iteration.name)

                                # allocation has been created - execute job
                                self.change_job_state(sched_job.job, iteration=job_iteration.iteration,
                                                      state=JobState.SCHEDULED)

                                self.queued_to_execute += 1
                                asyncio.ensure_future(self._executor.execute(allocation, job_iteration))
                            else:
                                # missing resources
                                _logger.debug("missing resources for job %s", job_iteration.name)
                                prev_iteration = job_iteration
                        except (NotSufficientResources, InvalidResourceSpec) as exc:
                            # jobs will never schedule
                            _logger.warning("Job %s scheduling failed - %s", job_iteration.name, str(exc))
                            sched_job.remove_iteration_job(job_iteration)
                            prev_iteration = None
                            self.change_job_state(sched_job.job, iteration=job_iteration.iteration,
                                                  state=JobState.FAILED, error_msg=str(exc))
                    else:
                        break

                if sched_job.has_more_iterations:
                    _logger.warning("Job %s preserved in scheduling queue", sched_job.job.name)
                    DirectManager._append_to_schedule_queue(new_schedule_queue, sched_job)

        self._schedule_queue = new_schedule_queue

    def change_job_state(self, job, iteration, state, error_msg=None):
        """Invoked to change job status.
        Any notification should be called from this method.

        Args:
            job (ExecutingJob): job that changed status
            iteration (int): job iteration index
            state (JobState): target job state
            error_msg (string): optional error messages
        """
        if self.stop_processing:
            return

        parent_job_changed_status = job.set_state(state, iteration, error_msg)

        self._fire_job_state_notifies(job.name, iteration, state)
        if parent_job_changed_status:
            _logger.debug("parent job %s status changed to %s - notifing", job.name, parent_job_changed_status.name)
            self._fire_job_state_notifies(job.name, None, state)

    def job_executing(self, job_iteration):
        """Invoked to signal starting job iteration execution.

        Args:
            job_iteration (SchedulingIteration): job iteration that started executing
        """
        if self.stop_processing:
            return

        self.change_job_state(job_iteration.job, iteration=job_iteration.iteration, state=JobState.EXECUTING)

    def job_finished(self, job_iteration, allocation, exit_code, error_msg, canceled=False):
        """Invoked to signal job finished.
        Allocation made for the job should be released.

        Args:
            job_iteration (SchedulingIteration): job iteration that finished
            allocation (Allocation): allocation created for the job
            exit_code (int): job exit code
            error_msg (str): an optional error message
        """
        if self.stop_processing:
            self._scheduler.release_allocation(allocation)
            return

        state = JobState.SUCCEED

        if canceled:
            state = JobState.CANCELED
        elif exit_code != 0:
            state = JobState.FAILED

        _logger.info('job %s finished with exit code %d, error message: %s', job_iteration.name, exit_code, error_msg)

        self.change_job_state(job_iteration.job, iteration=job_iteration.iteration, state=state, error_msg=error_msg)
        self._scheduler.release_allocation(allocation)
        self._schedule_loop()

    def _fire_job_state_notifies(self, job_id, iteration, state):
        """Create task with callback functions call registered for job state changes.
        A new asyncio task is created which call all registered callbacks in not defined order.

        Args:
            job_id (str): job identifier
            iteration (int): iteration index
            state (JobState): new job status
        """
        if len(self._job_states_cbs) > 0:
            _logger.debug("notifies callbacks about %s job status change %s",
                          job_id if iteration is None else '{}:{}'.format(job_id, iteration), state)
            asyncio.ensure_future(self._call_callbacks(job_id, iteration, state, self._job_states_cbs.values()))

    async def _call_callbacks(self, job_id, iteration, state, cbs):
        """Call job state change callback function with given arguments.

        Args:
            job_id (str): job identifier
            iteration (int): job iteration index
            state (JobState): new job status
            cbs ([]function): callback functions
        """
        if cbs is not None:
            for callb in cbs:
                try:
                    callb.callback(job_id, iteration, state, *callb.args)
                except Exception as exc:
                    _logger.exception("Callback function failed: %s", str(exc))

    def unregister_notifier(self, nid):
        """Unregister callback function for job state changes.

        Args:
            nid (str): the callback function identifier returned by ``register_notifier`` function

        Returns:
            bool: true if function unregistered successfully, and false if given identifier has not been found
        """
        if nid in self._job_states_cbs:
            del self._job_states_cbs[nid]
            return True

        return False

    def register_notifier(self, job_state_cb, *args):
        """Register callback function for job state changes.
        The registered function will be called for all job state changes.

        Args:
            job_state_cb (def): should accept two arguments - job name and new state

        Returns:
            str: identifier of registered callback, which can be used to unregister
              callback or None if callback function is missing or is invalid
        """
        if job_state_cb is not None:
            nid = uuid.uuid4()
            self._job_states_cbs[nid] = JobStateCB(job_state_cb, *args)

            return nid

        return None

    def register_jobs(self, jobs):
        """Register new jobs in jobs registry.

        Args:
            jobs (list(Job)): job descriptions to add to the registry

        Raises:
            JobAllreadyExist: when job with the same name was enqued earlier.
        """
        if jobs is not None:
            for job in jobs:
                self.job_list.add(job)

    def enqueue(self, jobs):
        """Enqueue job to execution.

        Args:
            jobs (list(Job)): job descriptions to add to the scheduler
        """
        if jobs is not None:
            for job in jobs:
                DirectManager._append_to_schedule_queue(self._schedule_queue, SchedulingJob(self, job))

            self._schedule_loop()

    def _remove_job_from_schedule_queue(self, job):
        """Remove job from schedule queue.

        Args:
            job (Job): job to remove from schedule queue

        Note: even if job doesn't exist in queue, the function do not raise exception.
        """
        try:
            # remove whole job from scheduling queue
            job_pos = next(i for i, sched_job in enumerate(self._schedule_queue) if sched_job.job == job)
            self._schedule_queue.pop(job_pos)
        except StopIteration:
            # ignore as the job could already be removed from scheduling queue (all iterations has been scheduled)
            _logger.info(f"job {job.name} doesn't exist in scheduling queue")

    def _remove_iteration_from_schedule_queue(self, job, iteration):
        """Remove iteration from schedule queue.

        Args:
            job (Job): job to remove from schedule queue
            iteration (int): iteration index

        Note: even if job doesn't exist in queue, the function do not raise exception.
        """
        try:
            # remove iteration (if generated) from scheduling job
            sched_job = next(sched_job for sched_job in self._schedule_queue if sched_job.job == job)
            sched_job.remove_iteration_index(iteration)
        except StopIteration:
            _logger.warning(f"job {job.name} doesn't exist in scheduling queue")

    def cancel_iteration(self, job, iteration):
        """Cancel single iteration.

        Args:
            job (Job): job to cancel
            iteration (int): iteration index
        """
        if job.state(iteration) == JobState.QUEUED:
            _logger.info(f'canceling iteration {iteration} of job {job.name} in queued state')
            # job is in scheduling queue, but don't have assigned allocation
            self._remove_iteration_from_schedule_queue(job, iteration)
            self.change_job_state(job, iteration=iteration, state=JobState.CANCELED)
        elif job.state(iteration) in [JobState.SCHEDULED, JobState.EXECUTING]:
            _logger.info(f'canceling iteration {iteration} of job {job.name} in scheduled state')
            self._executor.cancel_iteration(job, iteration)
        else:
            _logger.info(f'can\'t cancel iteration {iteration} of job {job.name} in state {job.state()}')

    def cancel_job(self, job):
        """Cancel job.

        Args:
            job (Job): job to cancel
        """
        try:
            if job.canceled:
                raise InvalidRequest(f'Job {job.name} already canceled')

            job.canceled = True

            if job.state() == JobState.QUEUED:
                self._remove_job_from_schedule_queue(job)

            if job.has_iterations:
                # possible many iterations
                for it in job.iteration.iterations_gen():
                    self.cancel_iteration(job, it)
            else:
                # single iteration
                self.cancel_iteration(job, None)
        except Exception as e:
            _logger.error(f'failed to cancel job: {str(e)}')
            raise

    @staticmethod
    def _append_to_schedule_queue(queue, sched_job):
        """Append job in scheduling queue.

        Args:
            queue (list): the queue to add job
            sched_job (SchedlingJob): job to add to the queue
        """
        queue.append(sched_job)
        sched_job.job.set_queue_pos(len(queue) - 1)

    def _get_parent_manager_socket(self):
        """Create an asynchronous ZMQ socket to the governor manager.

        Returns:
            zmq.Socket: socket to the governor manager
        """
        parent_socket = zmq.asyncio.Context.instance().socket(zmq.REQ) #pylint: disable=maybe-no-member
        parent_socket.connect(self._parent_manager)
        parent_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        return parent_socket

    def _get_parent_manager_socket_sync(self):
        """Create an synchronous ZMQ socket to the governor manager.

        Returns:
            zmq.Socket: socket to the governor manager
        """
        parent_socket = zmq.Context.instance().socket(zmq.REQ) #pylint: disable=maybe-no-member
        parent_socket.connect(self._parent_manager)
        parent_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        return parent_socket

    async def _send_parent_request_with_valid_async_timeout(self, request, timeout):
        """Send a request to the governor manager with asynchronous socket.

        Args:
            request (dict): request to send
            timeout (int): timeout in seconds to wait for reply
        """
        out_socket = None

        try:
            out_socket = self._get_parent_manager_socket()

            await out_socket.send_json(request)
            msg = await asyncio.wait_for(out_socket.recv_json(), timeout)
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(
                    msg.get('message', '')))

            return msg
        except Exception:
            raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(
                str(sys.exc_info())))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    # ignore errors during cleanup
                    _logger.debug('failed to close socket: %s', str(sys.exc_info()))

    async def _send_parent_manager_request_with_valid_async(self, request):
        """Send a request to the governor manager with asynchronous socket.

        Args:
            request (dict): request to send
        """
        out_socket = None

        try:
            out_socket = self._get_parent_manager_socket()

            await out_socket.send_json(request)
            msg = await asyncio.wait_for(out_socket.recv_json(), 5)
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to send message to parent manager: {}'.format(
                    msg.get('message', '')))

            return msg
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass

    def _notify_parent_with_job(self, job_id, iteration, state):
        """Send notification to the governor manager about submited job's state change.

        Args:
            job_id (str): job identifier
            iteration (int): iteration index
            state (JobState): new job state
        """
        if self._parent_manager and state.is_finished() and iteration is None:
            # notify parent only about whole jobs, not a single iterations
            # send also the job attributes which are necessary to identify the job in governor manager
            try:
                job = self.job_list.get(job_id)
                req_data = {
                    'request': 'notify',
                    'entity': 'job',
                    'params': {
                        'name': job_id,
                        'state': state.name,
                        'attributes': job.attributes
                    }
                }
                asyncio.ensure_future(self._send_parent_manager_request_with_valid_async(req_data))
            except Exception:
                _logger.error('failed to send job notification to the parent manager: %s', sys.exc_info())
                _logger.error(traceback.format_exc())

    async def register_in_parent(self):
        """Register manager instance in parent governor manager.
        """
        await self._send_parent_request_with_valid_async_timeout({
            'request': 'register',
            'entity': 'manager',
            'params': {
                'id': self.manager_id,
                'address': self.zmq_address,
                'resources': self.resources.to_dict(),
                'tags': self.manager_tags,
            }
        }, 5)


class DirectManagerHandler:
    """Direct execution mode handler for manager.
    In this mode, the manager will try to execute all incoming tasks on available resources,
    without submiting them to other managers. Also, if defined, the notifications about
    tasks completion will be sent to the parent manager (the managers governor).

    Attributes:
        _manager (Manager): manager instance
        _finish_task (asyncio.Future): the finish task
        _receiver (Receiver): receiver instance
        start_time (DateTime): moment of start of the handler
    """

    def __init__(self, manager):
        """Initialize instance.

        Args:
            manager (Manager): manager instance
        """
        self._manager = manager

        self._finish_task = None
        self._receiver = None

        self.start_time = datetime.now()

    def set_receiver(self, receiver):
        """Set receiver.

        Args:
            receiver (Receiver): the receiver instance
        """
        self._receiver = receiver
        if self._receiver:
            self._manager.set_zmq_address(self._receiver.zmq_address)

    async def handle_register_req(self, iface, request): #pylint: disable=W0613
        """Handle register request.

        Currently not implemented

        Args:
            iface (Interface): interface which received request
            request (ControlReq): register request data

        Returns:
            Response: the response data
        """
        return Response.error('Register manager request not supported in this kind of manager (direct)')

    async def handle_control_req(self, iface, request): #pylint: disable=W0613
        """Handlder for control commands.
        Control commands are used to configure system during run-time.

        Args:
            iface (Interface): interface which received request
            request (ControlReq): control request data

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        if request.command == ControlReq.REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE:
            if self._finish_task is not None:
                return Response.error('Finish request already requested')

            self._finish_task = asyncio.ensure_future(self._wait_for_all_jobs())

        return Response.ok('{} command accepted'.format(request.command))

    async def handle_submit_req(self, iface, request): #pylint: disable=W0613
        """Handlder for job submission.
        Before job will be submited the in-depth validation will be proviede, e.g.: job name
        uniqness.

        Args:
            iface (Interface): interface which received request
            request (SubmitJobReqest): submit request data

        Returns:
            Response: the response data
        """
        # enqueue job in the manager
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        try:
            jobs = self._prepare_jobs(request.jobs)

            self._manager.tracer.new_submited_jobs(jobs)

            self._manager.register_jobs(jobs)
            self._manager.enqueue(jobs)

            data = {
                'submitted': len(jobs),
                'jobs': [job.get_name() for job in jobs]
            }

            return Response.ok('{} jobs submitted'.format(len(jobs)), data=data)
        except Exception as exc:
            _logger.exception('Submit error')
            return Response.error(str(exc))

    def _prepare_jobs(self, req_jobs):
        """Validate submit job description.

        Args:
            req_jobs (list(dict)): list of submited job descriptions
        """
        resources = self._manager.resources

        req_job_names = set()

        new_jobs = []
        for req in req_jobs:
            req_job = req['req']
            req_vars = req['vars']

            req_vars['jname'] = DirectManagerHandler._replace_variables_in_string(req_job['name'], req_vars)

            if any((c in req_vars['jname'] for c in ['$', '{', '}', '(', ')', '\'', '"', ' ', '\t', '\n'])):
                raise InvalidRequest('Job identifier \'({})\' contains invalid characters or unknown variables'.format(
                    req_vars['jname']))

            # default value for missing 'resources' definition
            if 'resources' not in req_job:
                req_job['resources'] = {'numCores': {'exact': 1}}

            try:
                req_job_vars = DirectManagerHandler._replace_variables(req_job, req_vars)

                # verify job name uniqness
                if self._manager.job_list.exist(req_job_vars['name']) or \
                        req_job_vars['name'] in req_job_names:
                    raise JobAlreadyExist('Job {} already exist'.format(req_job_vars['name']))

                new_job = Job(**req_job_vars)
                # validate resource requirements
                if not resources.check_min_job_requirements(new_job.resources):
                    raise InvalidRequest('Not enough resources for job {}'.format(req_vars['jname']))

                new_jobs.append(new_job)
                req_job_names.add(req_job_vars['name'])
            except InvalidRequest:
                raise
            except JobAlreadyExist as exc:
                raise exc
            except Exception as exc:
                _logger.exception('Wrong submit request: %s', str(exc))
                raise InvalidRequest('Wrong submit request: {}'.format(str(exc)))

        # verify job dependencies
        # TODO: code to verify job dependencies
        return new_jobs

    @staticmethod
    def _replace_variables(data, variables):
        """Replace variables within given JSON-serializable data structure.

        Args:
            data (dict): a data structure where the variables should be replaced
            variables (dict): a dictionary with variables
        """
        if variables is not None and len(variables) > 0:
            return json.loads(Template(json.dumps(data)).safe_substitute(variables))

        return data

    @staticmethod
    def _replace_variables_in_string(string, variables):
        """Replace variables within string.

        Args:
            string (str): a string where the variables should be replaced
            varables (dict): a dictionary with variables
        """
        if variables is not None and len(variables) > 0:
            return Template(string).safe_substitute(variables)

        return string

    async def handle_jobstatus_req(self, iface, request): #pylint: disable=W0613
        """Handler for job status checking.

        Args:
            iface (Interface): interface which received request
            request (JobStatusReq): job status request

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        result = {}

        for job_name in request.job_names:
            try:
                real_job_name, job_iteration = JobList.parse_jobname(job_name)

                job = self._manager.job_list.get(real_job_name)

                if job is None:
                    return Response.error('Job \'{}\' doesn\'t exist'.format(job_name))

                if job_iteration is not None:
                    if not job.iteration.in_range(job_iteration):
                        return Response.error('Unknown iteration {} for job {}'.format(job_iteration, real_job_name))

                result[job_name] = {'status': int(ResponseCode.OK), 'data': {
                    'jobName': job_name,
                    'status': str(job.str_state(job_iteration))
                }}
            except Exception as exc:
                result[job_name] = {'status': int(ResponseCode.ERROR), 'message': exc.args[0]}

        return Response.ok(data={'jobs': result})

    async def handle_jobinfo_req(self, iface, request): #pylint: disable=W0613
        """Handler for job info checking.

        Args:
            iface (Interface): interface which received request
            request (JobInfoReq): job status request

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        result = {}

        for job_name in request.job_names:
            try:
                real_job_name, job_iteration = JobList.parse_jobname(job_name)

                job = self._manager.job_list.get(real_job_name)

                if job is None:
                    return Response.error('Job {} doesn\'t exist'.format(request.job_names))

                if job_iteration is not None:
                    if not job.iteration.in_range(job_iteration):
                        return Response.error('Unknown iteration {} for job {}'.format(job_iteration, real_job_name))

                job_data = {
                    'jobName': job_name,
                    'status': str(job.str_state(job_iteration))
                }

                if job_iteration is None and job.has_iterations:
                    job_data['iterations'] = {
                        'start': job.iteration.start,
                        'stop': job.iteration.stop,
                        'total': job.iteration.iterations(),
                        'finished': job.iteration.iterations() - job.get_not_finished_iterations(),
                        'failed': job.get_failed_iterations()
                    }

                    if request.include_childs:
                        job_data['childs'] = []
                        for idx, subjob in enumerate(job.iteration_states):
                            info = {
                                'iteration': idx + job.iteration.start,
                                'state': subjob.state().name
                            }

                            subruntime = subjob.runtime()
                            if subruntime is not None and len(subruntime) > 0:
                                info['runtime'] = subruntime

                            job_data['childs'].append(info)

                if job.messages(job_iteration) is not None:
                    job_data['messages'] = job.messages()

                jruntime = job.runtime(job_iteration)
                if jruntime is not None and len(jruntime) > 0:
                    job_data['runtime'] = jruntime

                jhistory = job.history(job_iteration)
                if jhistory is not None and len(jhistory) > 0:
                    history_str = ''

                    for entry in jhistory:
                        history_str = '\n'.join([history_str, "{}: {}".format(str(entry[1]), entry[0].name)])

                    job_data['history'] = history_str

                result[job_name] = {'status': int(ResponseCode.OK), 'data': job_data}
            except Exception as exc:
                result[job_name] = {'status': int(ResponseCode.ERROR), 'message': exc.args[0]}

        return Response.ok(data={'jobs': result})

    async def handle_canceljob_req(self, iface, request): #pylint: disable=W0613
        """Handler for cancel job request.

        Args:
            iface (Interface): interface which received request
            request (CancelJobReq): cancel job request

        Returns:
            Response: the response data
        """
        canceled = 0
        errors = {}

        for job_name in request.job_names:
            try:
                real_job_name, job_iteration = JobList.parse_jobname(job_name)

                job = self._manager.job_list.get(real_job_name)

                if job is None:
                    raise Exception(f'Job/iteration {job_name} doesn\'t exist')

                if job_iteration is not None:
                    if not job.iteration.in_range(job_iteration):
                        raise Exception(f'Unknown iteration {job_iteration} for job {jreal_job_name}')

                if job.state(iteration=job_iteration).is_finished():
                    raise InvalidRequest('Job/iteration {job_name} already finished - can not be removed'.format(job_name))

                if job_iteration is not None:
                    _logger.info(f'handling cancel operation on iteration {job_iteration} of job {job.name}')
                    self._manager.cancel_iteration(job, job_iteration)
                else:
                    _logger.info(f'handling cancel operation on job {job.name}')
                    self._manager.cancel_job(job)

                canceled += 1
            except Exception as exc:
                _logger.exception(f'failed to cancel job: {str(exc)}')
                errors[job_name] = exc.args[0]

        data = {
            'canceled': canceled,
        }

        if len(errors) > 0:
            data['errors'] = errors

        return Response.ok(data=data)

    async def handle_removejob_req(self, iface, request): #pylint: disable=W0613
        """Handler for remove job request.

        Args:
            iface (Interface): interface which received request
            request (RemoveJobReq): remove job request

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        removed = 0
        errors = {}

        for job_name in request.job_names:
            try:
                job = self._manager.job_list.get(job_name)

                if job is None:
                    raise InvalidRequest('Job \'{}\' doesn\'t exist'.format(job_name))

                if not job.state().is_finished():
                    raise InvalidRequest('Job \'{}\' not finished - can not be removed'.format(job_name))

                self._manager.job_list.remove(job_name)
                removed += 1
            except Exception as exc:
                errors[job_name] = exc.args[0]

        data = {
            'removed': removed,
        }

        if len(errors) > 0:
            data['errors'] = errors

        return Response.ok(data=data)

    async def handle_listjobs_req(self, iface, request): #pylint: disable=W0613
        """Handler for list jobs request.

        Args:
            iface (Interface): interface which received request
            request (ListJobsReq): list jobs request

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        job_names = self._manager.job_list.jobs()

        _logger.info("got %d jobs from list", len(job_names))

        jobs = {}
        for job_name in job_names:
            job = self._manager.job_list.get(job_name)

            if job is None:
                return Response.error('One of the job \'{}\' doesn\'t exist in registry'.format(job_name))

            job_data = {
                'status': str(job.str_state())
            }

            if job.messages() is not None:
                job_data['messages'] = job.messages()

            if job.queue_pos() is not None:
                job_data['inQueue'] = job.queue_pos()

            jobs[job_name] = job_data
        return Response.ok(data={
            'length': len(job_names),
            'jobs': jobs,
        })

    async def handle_resourcesinfo_req(self, iface, request): #pylint: disable=W0613
        """Handler resources info request.

        Args:
            iface (Interface): interface which received request
            request (ResourcesReq): resources info request

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        resources = self._manager.resources
        return Response.ok(data={
            'total_nodes': resources.total_nodes,
            'total_cores': resources.total_cores,
            'used_cores': resources.used_cores,
            'free_cores': resources.free_cores
        })

    async def handle_finish_req(self, iface, request): #pylint: disable=W0613
        """Handler finish request.

        Args:
            iface (Interface): interface which received request
            request (FinishReq): job status request

        Returns:
            Response: the response data
        """
        delay = 2

        if self._finish_task is not None:
            return Response.error('Finish request already requested')

        self._finish_task = asyncio.ensure_future(self._delayed_finish(delay))

        return Response.ok(data={
            'when': '{}s'.format(delay)
        })

    async def generate_status_response(self):
        """Generate current status statistics.

        Returns:
            stats (dict): current status statistics
        """
        scheduling_jobs = failed_jobs = finished_jobs = executing_jobs = 0
        job_names = self._manager.job_list.jobs()
        for job_name in job_names:
            job = self._manager.job_list.get(job_name)

            if job is None:
                _logger.warning('missing job\'s %s data', job_name)
            else:
                if job.state() in [JobState.QUEUED, JobState.SCHEDULED]:
                    scheduling_jobs += 1
                elif job.state() in [JobState.EXECUTING]:
                    executing_jobs += 1
                elif job.state() in [JobState.FAILED, JobState.OMITTED]:
                    failed_jobs += 1
                elif job.state() in [JobState.CANCELED, JobState.SUCCEED]:
                    finished_jobs += 1

        resources = self._manager.resources
        return Response.ok(data={
            'System': {
                'Uptime': str(datetime.now() - self.start_time),
                'Zmqaddress': self._receiver.zmq_address,
                'Ifaces': [iface.name() for iface in self._receiver.interfaces] \
                    if self._receiver and self._receiver.interfaces else [],
                'Host': socket.gethostname(),
                'Account': getpass.getuser(),
                'Wd': os.getcwd(),
                'PythonVersion': sys.version.replace('\n', ' '),
                'Python': sys.executable,
                'Platform': sys.platform,
            }, 'Resources': {
                'TotalNodes': resources.total_nodes,
                'TotalCores': resources.total_cores,
                'UsedCores': resources.used_cores,
                'FreeCores': resources.free_cores,
            }, 'JobStats': {
                'TotalJobs': len(job_names),
                'InScheduleJobs': scheduling_jobs,
                'FailedJobs': failed_jobs,
                'FinishedJobs': finished_jobs,
                'ExecutingJobs': executing_jobs,
            }})

    async def handle_status_req(self, iface, request): #pylint: disable=W0613
        """Handler status request.

        Args:
            iface (Interface): interface which received request
            request (StatusReq): current system status request

        Returns:
            Response: the response data
        """
        if self._manager.stop_processing:
            return Response.error('processing stopped')

        if request.allJobsFinished:
            return Response.ok(data={'AllJobsFinished': self._manager.is_all_jobs_finished})
        else:
            return await self.generate_status_response()

    async def handle_notify_req(self, iface, request): #pylint: disable=W0613
        """Handler for notify request.

        Args:
            iface (Interface): interface which received request
            request (NotifyReq): notify request

        Returns:
            Response: the response data
        """
        return Response.error('Operation not supported')

    async def _wait_for_all_jobs(self):
        """Wait until all jobs finish and stop receiver."""
        _logger.info("waiting for all jobs to finish (the new method)")

        while not self._manager.is_all_jobs_finished:
            await asyncio.sleep(0.2)

        _logger.info("detected all jobs finished")
        self.stop_receiver()

    async def _delayed_finish(self, delay):
        """Stop receiver with given delay.

        Args:
            delay (int): number of seconds to wait before stoping receiver
        """
        _logger.info("finishing in %d seconds", delay)

        await asyncio.sleep(delay)

        self.stop_receiver()

    def stop_receiver(self):
        """Signal receiver to stop."""
        if self._receiver:
            self._receiver.set_finish(True)
        else:
            _logger.warning('Failed to set finish flag due to lack of receiver access')
