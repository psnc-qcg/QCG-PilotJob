import logging

from qcg.pilotjob.common.errors import IllegalJobDescription
from qcg.pilotjob.inputqueue.job import Job, JobState
from qcg.pilotjob.inputqueue.jobdb import job_db
from qcg.pilotjob.inputqueue.publisher import Publisher, EventTopic




class IQScheduledJob:

    # how many iterations should be resolved at each scheduling step
    ITERATIONS_SPLIT = 100

    def __init__(self, job):
        """The scheduling information about job.

        Args:
            job (qcg.pilotjob.inputqueue.job.Job): the job db object
        """
        self.job = job

        # has the job chance to meet dependencies
        self.feasible = True

        # list of dependant jobs (without individual for each subjob) - common for all iterations
        # this list is modified in `self.check_dependencies` and `self.depend_job_status_finished`
        # so when all general job deps are met this list should be empty
        self.after_jobs = set()

        # list of dependant individual subjobs for each subjob - specific for each iteration
        # due to the bit masks in `self.iterations_deps_mask` - this list can not be modified
        # to check if individual iterations deps are met, the `self.iterations_deps_mask` should
        # be checked
        self.after_iteration_jobs = list()

        # flag for iterative jobs
        self.has_iterations = self.job.has_iterations

        # total number of iterations
        self.total_iterations = self.job.iteration.iterations() if self.has_iterations else 1

        # a bit map list of specific iteration dependencies; each iteration (position in list)
        # contain bit map, where `n's` 1 bit means non met dependencies for iteration dep on position `n` in
        # `self.after_iteration_jobs`
        # set only where exists specific iteration dependencies (`self.after_iteration_jobs` contains elements)
        self.iterations_deps_mask = None

        # list of number of non met specific iteration dependencies for iteration dep on position
        # `self.after_iteraiton_jobs`
        # set only where exists specific iteration dependencies (`self.after_iteration_jobs` contains elements)
        self.iterations_deps_count = None

        # number of omitted iterations
        # set only where exists specific iteration dependencies (`self.after_iteration_jobs` contains elements)
        self.iterations_deps_omitted = 0

        # a number of not-ready iterations
        self.waiting_iterations_count = 0

        # general dependencies
        if job.has_dependencies:
            for job_id in job.description.dependencies.after:
                if job_id.endswith(':${it}'):
                    # for iteration dependencies, record only job name
                    after_job_id = job_id.split(':', 1)[0]
                    self.after_iteration_jobs.append(after_job_id)
                else:
                    self.after_jobs.add(job_id)

            if self.after_iteration_jobs:
                if len(self.after_iteration_jobs) > 16:
                    raise IllegalJobDescription(f'Too large number of iteration dependencies for job '
                                                f'{self.job.get_name()} ({len(self.after_iteration_jobs)} exceeds '
                                                f'maximum number 16)')
                dep_bit_mask = (1<<len(self.after_iteration_jobs)) - 1
                self.iterations_deps_mask = [dep_bit_mask for _ in range(self.total_iterations)]
                self.iterations_deps_count = [self.total_iterations for _ in range(len(self.after_iteration_jobs))]

            self.check_dependencies()

        # compute minimum resources size
        self.min_res_cores = job.description.resources.get_min_num_cores()
        self.min_res_node_cores = job.description.resources.get_min_node_num_cores()
        self.min_res_nodes = job.description.resources.get_min_nodes()

        logging.debug(f'minimum # of cores for job {self.job.get_name()} is {self.min_res_cores}, '
                      f'per node {self.min_res_node_cores}')

    def is_feasible(self):
        """Return if job has chance to execute.
        If some dependent job fail, or all iteration specific dependants fail job will never execute.
        """
        return self.feasible and self.iterations_deps_omitted < self.total_iterations

    def has_dependencies(self):
        """Return if job has any dependencies left.

        Return:
            bool: True if some dependencies has not been yet met, otherwise False
        """
        return len(self.after_jobs) > 0 or (
                self.iterations_deps_count is not None and any(count > 0 for count in self.iterations_deps_count))

    def get_dependency_jobs(self):
        """Return list of all jobs the current job depends on."""
        if self.after_iteration_jobs:
            return self.after_jobs | set([self.after_iteration_jobs[dep_id] for dep_id,_ in
                                          enumerate(self.after_iteration_jobs)
                                          if self.iterations_deps_count[dep_id] > 0])
        else:
            return self.after_jobs

    def is_depends_on(self, job_name):
        """Check if current job depends on given job name.

        Args:
            job_name (str): job to check in dependencies

        Returns:
            bool: True current job depends on give job name, False otherwise.
        """
        return job_name in self.after_jobs or\
               (self.after_iteration_jobs is not None and
                job_name in self.after_iteration_jobs and
                self.iterations_deps_count[self.after_iteration_jobs.index(job_name)] > 0)

    def check_dependencies(self):
        """Update dependency state.
        Check all dependent jobs and update job's ready (and possible feasible) status.
        Normally to update dependency state the `depend_job_status_finished` method should be called.
        This method should be called once at job scheduling initialization, and followed by
        `depend_job_status_finished` accordingly to job status changes.
        """
        # check general job dependencies
        if self.after_jobs:
            finished = set()

            for job_id in self.after_jobs:
                job_name = job_id
                job_it = None

                if ":" in job_name:
                    job_name, job_it = Job.parse_jobname(job_name)

                dep_job = job_db.get(job_name)
                if dep_job is None:
                    logging.warning(f"Dependency job '{job_id}' not registered")
                    self.feasible = False
                    break

                dep_job_state = dep_job.state(job_it)
                if dep_job_state.is_finished():
                    if dep_job.state() != JobState.SUCCEED:
                        self.feasible = False
                        break

                    finished.add(job_id)

                self.after_jobs -= finished

        # check specific iteration's dependencies
        if self.after_iteration_jobs:
            for dep_id, job_name in enumerate(self.after_iteration_jobs):
                dep_job = job_db.get(job_name)

                for it_idx, it_state in enumerate(dep_job.iteration_states):
                    if it_state.is_finished():
                        self.iterations_deps_count[dep_id] -= 1

                        # check if iteration is already omitted
                        if self.iterations_deps_mask[it_idx] != -1:
                            if it_state != JobState.SUCCEED:
                                # the iteration is omitted and should never execute
                                self.iterations_deps_mask[it_idx] = -1
                                self.iterations_deps_omitted += 1
                            else:
                                # the bitwise negation in Python is sick
                                self.iterations_deps_mask[it_idx] &= (1<<16) - 1 - (2**dep_id)

                job_state = dep_job.state()
                if job_state.is_finished():
                    # if job is finished no it's iteration should change status
                    self.iterations_deps_count[dep_id] = 0


    def depend_job_status_finished(self, depend_job, iteration):
        """The parent job current job depends on changed it's status or it's iteration changed status.
        This method will update currently not met dependencies.

        Args:
            depend_job (qcg.pilotjob.inputqueue.job.Job): job that changed status
            iteration (int): optional iteration index that changed status (non normalized)

        Returns:
            (list(int), list(int)): a pair of two lists:
                newly_ready_iterations - an iterations that are ready for execution (normalized)
                newly_infeasible_iterations - an iterations that are infeasible - will never execute (normalized)
        """
        dep_job_state = depend_job.state(iteration)

        if iteration and depend_job.get_name() in self.after_iteration_jobs:
            # compute iteration index relative to 0
            iteration_idx = depend_job.iteration.normalize(iteration)

            # get position of the dependency job in `self.after_iteration_jobs`
            dep_id = self.after_iteration_jobs.index(depend_job.get_name())

            if self.iterations_deps_count[dep_id]:
                self.iterations_deps_count[dep_id] -= 1

                if self.iterations_deps_mask[iteration_idx] != -1:
                    if dep_job_state != JobState.SUCCEED:
                        # an iteration infeasible - should never run
                        self.iterations_deps_mask[iteration_idx] = -1
                        self.iterations_deps_omitted += 1
                        return None, [iteration_idx]

                    # the bitwise negation in Python is sick
                    self.iterations_deps_mask[iteration_idx] &= (1<<16) - 1 - (2**dep_id)

                    # check if iteration is read to execute
                    if len(self.after_jobs) == 0 and self.iterations_deps_mask[iteration_idx] == 0:
                        # iteration status changed
                        return [iteration_idx], None
        elif depend_job.get_name(iteration) in self.after_jobs:
            if dep_job_state != JobState.SUCCEED:
                # job infeasible - should never run
                self.feasible = False
                return None, None

            self.after_jobs.remove(depend_job.get_name(iteration))

            if len(self.after_jobs) == 0:
                if len(self.after_iteration_jobs) > 0:
                    # job met all dependencies but there might be iteration dependencies
                    return [it_idx for it_idx in range(self.total_iterations)
                            if self.iterations_deps_mask[it_idx] == 0], None
                else:
                    # job met all dependencies
                    return range(self.total_iterations), None

        return None, None

    def get_initial_ready_iterations(self):
        if len(self.after_jobs) == 0:
            if self.iterations_deps_mask:
                return [it_idx for it_idx, it_mask in enumerate(self.iterations_deps_mask) if it_mask == 0]
            else:
                return range(self.total_iterations)
        else:
            return list()

    def get_infeasible_iterations(self):
        if self.iterations_deps_mask:
            return [it_idx for it_idx, it_mask in enumerate(self.iterations_deps_mask) if it_mask == -1]

        return list()


class ReadyIterations:
    """A class that represents iterations ready for execution."""

    def __init__(self, job, iterations):
        """A job with iterations that are ready for execution.

        Args:
            job (IQScheduledJob): job iterations belong to
            iterations (list(int)): list of ready iterations
        """
        self.job = job
        self.iterations = set(iterations)

    def allocate_max_cores(self, max_cores):
        """Allocate iterations.
        This method will return AND remove from `self.iterations` set of iterations that altogether requires
        `max_cores`.

        Args:
            max_cores (int): the maximum total number of cores required by all allocated iterations

        Return:
            list(int), int: a pair with set of iteration indexes and total number of required cores by all iterations
                (in result set)
        """
        alloc_cores = min(len(self.iterations) * self.job.min_res_cores, max_cores)
        alloc_iters = int(alloc_cores / self.job.min_res_cores)

        print(f'allocate_max_cores ({max_cores}) for job ({self.job.job.get_name()}): '
              f'len_iterations({len(self.iterations)}), '
              f'job_min_res_cores({self.job.min_res_cores}), alloc_cores({alloc_cores}), '
              f'alloc_iters({alloc_iters})')
        return [self.iterations.pop() for _ in range(alloc_iters)], alloc_iters * self.job.min_res_cores

    def has_iterations(self):
        """Check if there are any ready iterations.

        Returns:
            int: number of ready iterations
        """
        return len(self.iterations)


class IQManager:

    def __init__(self, config):
        """The Input queue manager class queues all incoming job submit requests and governs job dependencies.
        All ready job iterations (without dependencies or with all dependencies met) are stored in `ready_iterations`
        map and are available for executors. To properly manage job dependencies, the `IQManager` must be notified
        about all finished job iterations via `job_finished` method.
        """

        # a list of jobs with not fully met dependencies
        self.non_ready_jobs = dict()

        # a list of job iterations that are ready for execution
        self.ready_iterations = dict()

        # a map with job names that are defined as dependencies and list of jobs that depends on them
        self.tracked_jobs = dict()

        # a map of executors
        self.executors = dict()

        # an event publisher
        self.event_publisher = Publisher()
        self.event_publisher.setup(config)

    def stop(self):
        logging.info('stopping iq manager')
        self.event_publisher.stop()

    def register_executor(self, executor):
        if executor.name in self.executors:
            logging.warning(f'executor {executor.name} already registered - overwriting')

        self.executors[executor.name] = executor

    def executor_heartbeat(self, executor_name):
        executor = self.executors.get(executor_name)
        if not executor:
            logging.warning(f'executor {executor_name} not known')
            return

        executor.heart_beat()

    def manager_info(self):
        return {
            'ready_its': len(self.ready_iterations),
            'events': self.event_publisher.external_address
        }

    def executor_reserve_jobs(self, executor_name, allocation=dict()):
        executor = self.executors.get(executor_name)
        if not executor:
            logging.warning(f'executor {executor_name} not known')

        ex_res = executor.resources
        max_cores = allocation.get('max_cores', ex_res.get('total_cpus'))

        reserved_iterations, _ = self.get_ready_iterations(max_cores,
                                                           ex_res.get('total_cpus'),
                                                           ex_res.get('node_max_cpus'),
                                                           ex_res.get('total_nodes'))

        jobs_to_execute = []
        for scheduled_job, iteration_list in reserved_iterations.items():
            jobs_to_execute.append({'job': scheduled_job.job.description.to_dict(),
                                    'iterations': iteration_list})

        logging.info(f'reserved for executor {executor_name} iterations {reserved_iterations}')
        executor.append_allocation(reserved_iterations)
        return jobs_to_execute

    def get_ready_iterations(self, max_cores, max_cpu_cores, max_node_cpu_cores, max_nodes):
        """Return iterations ready to execute.

        Args:
            max_cores (int): maximum number of cores the all iterations should require
            max_cpu_cores (int): maximum number of cores per single iteration
            max_node_cpu_cores (int): maximum number of cores per node per iteration
            max_nodes (int): maximum number of nodes per iteration

        Return:
            list(int): list with ready iterations that meets requirements or None if there is no
                ready iteration that meets requirement
        """
        rest_cores = max_cores
        allocation = {}
        empty_its = []

        for jname, ready_its in self.ready_iterations.items():
            print(f'comparing required max_cpu_cores({max_cpu_cores}), max_node_cpu_cores({max_node_cpu_cores})'
                          f', max_nodes({max_nodes}) with job\'s min_res_cores({ready_its.job.min_res_cores}), '
                          f'min_res_node_cores({ready_its.job.min_res_node_cores}), '
                          f'min_res_nodes({ready_its.job.min_res_nodes})')
            if all((max_cpu_cores >= ready_its.job.min_res_cores,
                    max_node_cpu_cores >= ready_its.job.min_res_node_cores,
                    max_nodes >= ready_its.job.min_res_nodes)):
                print('ok')
                core_list, alloc_cores = ready_its.allocate_max_cores(rest_cores)

                allocation[ready_its.job] = core_list
                print(f'allocated {alloc_cores} from job {ready_its.job.job.get_name()}')
                rest_cores -= alloc_cores

                if not ready_its.has_iterations() and jname not in self.non_ready_jobs:
                    empty_its.append(jname)

                if rest_cores == 0:
                    break
            else:
                print('not match')

        for jid in empty_its:
            del self.ready_iterations[jid]

        return allocation, max_cores - rest_cores

    def enqueue_jobs(self, jobs):
        """Add jobs to the input queue.
        Firstly, all job are added to the database. Then it's dependencies are checked and all
        ready iterations are putted to the ready iterations queue. For the non-ready iterations,
        the dependency tracking job is initialized, and after each tracked job status changed
        each job's dependencies are updated.

        Args:
            jobs (list(qcg.pilotjob.inputqueue.job.Job)): list of jobs to enqueue
        """
        # first add all jobs to the db to make sure all dependent jobs in current batch are in db
        for job in jobs:
            job_db.add(job)

        # check job dependencies and schedule them to proper queues
        for job in jobs:
            iq_job = IQScheduledJob(job)

            if not iq_job.is_feasible():
                # job dependencies will never be met - change job state
                # do not bother further this job
                job.change_state(JobState.OMITTED)
            else:
                # check if there are any infeasible iterations
                for infeasible_iteration in iq_job.get_infeasible_iterations():
                    iq_job.job.change_state(JobState.OMITTED, iq_job.job.iteration.start + infeasible_iteration)

                # put currently ready iterations to the `ready_iterations` map
                self.ready_iterations[iq_job.job.get_name()] =\
                    ReadyIterations(iq_job, iq_job.get_initial_ready_iterations())
                self.event_publisher.publish(topic=EventTopic.NEW_JOBS,
                                             data={'ready_its': len(self.ready_iterations)})

                # if job has not met dependencies, track jobs it depends on
                if iq_job.has_dependencies():
                    self.non_ready_jobs[iq_job.job.get_name()] = iq_job

                    for dep_job in iq_job.get_dependency_jobs():
                        self.tracked_jobs.setdefault(dep_job, []).append(iq_job)

    def job_finished(self, job_name, iteration=None):
        """Method should be called when job or it's iteration finish.
        It is used to update dependency requirements of pending jobs.

        Args:
            job_name (str): job name
            iteration (int): if defined the {iteration}'th of the job finished
        """
        try:
            if job_name in self.tracked_jobs:
                depend_job = job_db.get(job_name)

                for iq_job in self.tracked_jobs.get(job_name, []):
                    newly_ready_iterations, newly_infeasible_iterations =\
                        iq_job.depend_job_status_finished(depend_job, iteration)

                    if newly_ready_iterations:
                        # update ready iterations for this job
                        self.ready_iterations[iq_job.job.get_name()].iterations.update(newly_ready_iterations)
                        self.event_publisher.publish(topic=EventTopic.NEW_JOBS,
                                                     data={'ready_its': len(self.ready_iterations)})

                    if newly_infeasible_iterations:
                        for infeasible_iteration in newly_infeasible_iterations:
                            iq_job.job.change_state(JobState.OMITTED, iq_job.job.iteration.start + infeasible_iteration)

                    if not iq_job.is_feasible():
                        # job dependencies will never be met - change job state
                        # do not bother further this job
                        self.delete_tracked_job(job_name, iq_job)
                        del self.ready_iterations[iq_job.job.get_name()]
                        iq_job.job.change_state(JobState.OMITTED)
                        del self.non_ready_jobs[iq_job.job.get_name()]
                    else:
                        if not iq_job.is_depends_on(job_name):
                            self.delete_tracked_job(job_name, iq_job)

                        if not iq_job.has_dependencies():
                            # if all dependencies has been met remove job from `non_ready_jobs` set
                            del self.non_ready_jobs[iq_job.job.get_name()]

        except KeyError:
            logging.exception(f'unknown job {job_name}')

    def delete_tracked_job(self, tracked_job_name, iq_job):
        """Delete tracking job entry.

        Args:
            tracked_job_name (str): the job name to stop tracking
            iq_job (qcg.pilotjob.inputqueue.iqmanager.IQScheduledJob): the input queue job for which stop tracking job
                iteration status changes.
        """
        if tracked_job_name in self.tracked_jobs:
            self.tracked_jobs[tracked_job_name].remove(iq_job)
            if len(self.tracked_jobs[tracked_job_name]) == 0:
                del self.tracked_jobs[tracked_job_name]

