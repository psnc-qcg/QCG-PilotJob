import json
import re
import logging

from datetime import datetime, timedelta
from enum import Enum

from qcg.pilotjob.resources import CRType
from qcg.pilotjob.errors import JobAlreadyExist, IllegalResourceRequirements, IllegalJobDescription


_logger = logging.getLogger(__name__)


class JobState(Enum):
    """The job state."""

    QUEUED = 1
    SCHEDULED = 2
    EXECUTING = 3
    SUCCEED = 4
    FAILED = 5
    CANCELED = 6
    OMITTED = 7

    def is_finished(self):
        """Check if job state is finished (final)."""
        return self in [JobState.SUCCEED, JobState.FAILED, JobState.CANCELED,
                        JobState.OMITTED]

    def stats(self, stats):
        if self in [JobState.QUEUED, JobState.SCHEDULED]:
            stats['scheduling'] = stats.get('scheduling', 0) + 1
        elif self in [JobState.EXECUTING]:
            stats['executing'] = stats.get('executing', 0) + 1
        elif self in [JobState.FAILED, JobState.OMITTED]:
            stats['failed'] = stats.get('failed', 0) + 1
        elif self in [JobState.CANCELED, JobState.SUCCEED]:
            stats['finished'] = stats.get('finished', 0) + 1


class JobExecution:
    """The execution element of job description.

    Attributes:
        exec (str, optional): path to the executable
        args (list(str), optional): list of arguments
        env (dict(str, str), optional): list of environment variables
        stdin (str, optional): path to the standard input file
        stdout (str, optional): path to the standard output file
        stderr (str, optional): path to the standard error file
        modules (list(str), optional): list of modules to load before job start
        venv (str, optional): path to the virtual environment to initialize before job start
        wd (str, optional): path to the job's working directory
        model (str, optional): model of execution
    """

    def __init__(self, exec=None, args=None, env=None, script=None, stdin=None, stdout=None, stderr=None,
                 modules=None, venv=None, wd=None, model=None):
        """Initialize execution element of job description.

        Args:
            exec (str, optional): path to the executable
            args (list(str), optional): list of arguments
            env (dict(str, str), optional): list of environment variables
            stdin (str, optional): path to the standard input file
            stdout (str, optional): path to the standard output file
            stderr (str, optional): path to the standard error file
            modules (list(str), optional): list of modules to load before job start
            venv (str, optional): path to the virtual environment to initialize before job start
            wd (str, optional): path to the job's working directory
            model (str, optional): model of execution

        Raises:
            IllegalJobDescription: when:
                * nor ``exec`` or ``script`` are defined,
                * ``script`` and one of ``exec``, ``args`` or ``env`` is both defined
                * ``args`` is not a list
                * ``env`` is not a dictionary
        """
        if all((not exec, not script)):
            raise IllegalJobDescription("Job execution (exec or script) not defined")

        if script and (exec or args or env):
            raise IllegalJobDescription("Job script and exec or args or env defined")

        self.exec = exec
        self.script = script

        self.args = []
        self.env = {}

        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        if modules and not isinstance(modules, list):
            self.modules = [modules]
        else:
            self.modules = modules

        self.venv = venv

        if model and isinstance(model, str):
            self.model = model.lower()
        else:
            self.model = model

        if args is not None:
            if not isinstance(args, list):
                raise IllegalJobDescription("Execution arguments must be an array")
            self.args = args

        if env is not None:
            if not isinstance(env, dict):
                raise IllegalJobDescription("Execution environment must be an dictionary")
            self.env = env

        self.wd = wd

    def to_dict(self):
        """Serialize ``execution`` element to dictionary.

        Returns:
            dict(str): dictionary with ``execution`` element values
        """
        result = {'exec': self.exec, 'args': self.args, 'env': self.env, 'script': self.script}
        if self.wd is not None:
            result['wd'] = self.wd

        if self.stdin is not None:
            result['stdin'] = self.stdin

        if self.stdout is not None:
            result['stdout'] = self.stdout

        if self.stderr is not None:
            result['stderr'] = self.stderr

        if self.modules is not None:
            result['modules'] = self.modules

        if self.venv is not None:
            result['venv'] = self.venv

        if self.model is not None:
            result['model'] = self.model

        return result

    def to_json(self):
        """Serialize ``execution`` element to JSON description.

        Returns:
            JSON description of ``execution`` element.
        """
        return json.dumps(self.to_dict())


class ResourceSize:
    """The resources size element used in job description when specified the number of required cores or nodes."""

    def __init__(self, exact=None, min=None, max=None, scheduler=None):
        """Initialize resource size.

        Args:
            exact (int, optional): exact number of resources
            min (int, optional): minimum number of resources
            max (int, optional): maximum number of resources
            scheduler (dict, optional): the iteration resources scheduler, the ``name`` and ``params`` (optional) keys

        Raises:
            IllegalResourceRequirements raised when:
                * ``exact`` number and one of ``min``, ``max`` or ``scheduler`` is both specified
                * nor ``exact`` or ``min`` or ``max`` is not specified
                * ``max`` and ``min`` is specified and ``min`` > ``max``
        """
        if exact is not None and (min is not None or max is not None or scheduler is not None):
            raise IllegalResourceRequirements("Exact number of resources defined with min/max/scheduler")

        if max is not None and min is not None and min > max:
            raise IllegalResourceRequirements("Maximum number greater than minimal")

        if exact is None and min is None and max is None:
            raise IllegalResourceRequirements("No resources defined")

        if (exact is not None and exact < 0) or (min is not None and min < 0) or (max is not None and max < 0):
            raise IllegalResourceRequirements("Neative number of resources")

        self._exact = exact
        self._min = min
        self._max = max
        self._scheduler = scheduler

    @property
    def exact(self):
        """ int: exact number of resources."""
        return self._exact

    @property
    def min(self):
        """int: minimum number of resources"""
        return self._min

    @property
    def max(self):
        """int: maximum number of resources"""
        return self._max

    @property
    def scheduler(self):
        """str: iteration resource scheduler name"""
        return self._scheduler

    @property
    def range(self):
        """(int, int): tuple with resources range"""
        return self._min, self._max

    def is_exact(self):
        """Check if resource size is defined as exact number.

        Returns:
            True: if resource size is defined as exact number
            False: if reosurce size is defined as range
        """
        return self._exact is not None

    def to_dict(self):
        """Serialize resource size to dictionary

        Returns:
            dict(str): dictionary with resource size
        """
        result = {}

        if self._exact is not None:
            result['exact'] = self._exact

        if self._min is not None:
            result['min'] = self._min

        if self._max is not None:
            result['max'] = self._max

        if self._scheduler is not None:
            result['scheduler'] = self._scheduler

        return result

    def to_json(self):
        """Serialize resource size to JSON description.

        Returns:
         JSON description of resource size element.
        """
        return json.dumps(self.to_dict())


class JobResources:
    """The ```resources``` element of job description."""

    _wt_regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

    def _parse_wt(self, wt):
        """Parse wall time description into timedelta structure.

        Args:
            wt (str): the wall time description as a string

        Returns:
            timedelta: parsed wall time description

        Raises:
            IllegalResourceRequirements: when wall time description has wrong format.
        """
        parts = self._wt_regex.match(wt)
        if not parts:
            raise IllegalResourceRequirements("Wrong wall time format")

        try:
            parts = parts.groupdict()
            time_params = {}
            for name, param in parts.items():
                if param:
                    time_params[name] = int(param)

            td = timedelta(**time_params)
            if td.total_seconds() == 0:
                raise IllegalResourceRequirements("Wall time must be greater than 0")

            return td
        except IllegalResourceRequirements:
            raise
        except Exception:
            raise IllegalResourceRequirements("Wrong wall time format")

    @staticmethod
    def _validate_crs(crs):
        """Validate consumable resources.

        Check if crs are known and well defined.

        Args:
            crs (dict(string, int)): map with consumable resources.

        Returns:
            dict: final map of crs

        Raises:
            IllegalResourceRequirements: when
                * unknown consumable resources
                * double definition of consumable resources
                * number of consumable resources is not defined as integer
                * number of consumable resources is less than 1
        """
        if crs:
            # already used crs
            result = dict()

            for name, count in crs.items():
                if not name.upper() in CRType.__members__:
                    raise IllegalResourceRequirements("Unknown consumable resource {}".format(name))

                cr_type = CRType[name.upper()]
                if cr_type in result:
                    raise IllegalResourceRequirements("Consumable resource {} already defined".format(name))

                if not isinstance(count, int):
                    raise IllegalResourceRequirements("Consumable resource {} count not a number {}".format(
                        name, type(count).__name__))

                if count < 1:
                    raise IllegalResourceRequirements("Number of consumable resource {} must be greater than 0".format(
                        name))

                result[cr_type] = count

        return result

    def __init__(self, numCores=None, numNodes=None, wt=None, nodeCrs=None):
        """Initialize ``resources`` element of job description.

        * if numNodes > 1, then numCores relates to each of the node, so total number of
                required cores will be a product of numNodes and numCores
        * nodeCrs relates to each node available consumable resources

        Args:
            numCores - number of cores, either as exact number or as a range
            numNodes - number of nodes, either as exact number of as a range
            wt - wall time
            nodeCrs (dict(string,int)) - each node consumable resources

        Raises:
            IlleglResourceRequirements: raised when:
                * ``numCores`` and ``numNodes`` not defined
                * ``numCores`` or ``numNodes`` not instance of either ``int``, ``dict`` or ResourceSize
                * wrong consumable resources definition
        """
        if numCores is None and numNodes is None:
            raise IllegalResourceRequirements("No resources defined")

        if numCores is not None:
            if isinstance(numCores, int):
                numCores = ResourceSize(numCores)
            elif isinstance(numCores, dict):
                numCores = ResourceSize(**numCores)
            elif not isinstance(numCores, ResourceSize):
                raise IllegalJobDescription("Wrong definition of number of cores (%s)" % (type(numCores).__name__))

        if numNodes is not None:
            if isinstance(numNodes, int):
                numNodes = ResourceSize(numNodes)
            elif isinstance(numNodes, dict):
                numNodes = ResourceSize(**numNodes)
            elif not isinstance(numNodes, ResourceSize):
                raise IllegalJobDescription("Wrong definition of number of nodes (%s)" % (type(numNodes).__name__))

        if wt is not None:
            self.wt = self._parse_wt(wt)
        else:
            self.wt = None

        self._crs = None
        if nodeCrs is not None:
            if not isinstance(nodeCrs, dict):
                raise IllegalJobDescription("Wrong definition of Consumable Resources {} (must be a dictionary)".format(
                    type(nodeCrs).__name__))

            self._crs = JobResources._validate_crs(nodeCrs)

        self._cores = numCores
        self._nodes = numNodes

    @property
    def has_nodes(self):
        """bool: true if ``resources`` element of job description contains number of nodes definition"""
        return self._nodes is not None

    @property
    def has_cores(self):
        """bool: true if ``resources`` element of job description contains number of cores definition"""
        return self._cores is not None

    @property
    def has_crs(self):
        """bool: true if ``resources`` element of job description contains consumable resources definition"""
        return self._crs is not None

    @property
    def cores(self):
        """ResourceSize: return ``numCores`` definition of ``resources`` element."""
        return self._cores

    @property
    def nodes(self):
        """ResourceSize: return ``numNodes`` definition of ``resources`` element."""
        return self._nodes

    @property
    def crs(self):
        """ResourceSize: return ``nodeCrs`` definition of ``resources`` element."""
        return self._crs

    def get_min_num_cores(self):
        """Return minimum number of cores the job can be run.

        Returns:
            int: minimum number of required cores for the job.
        """
        min_cores = 1
        if self.has_cores:
            if self._cores.is_exact():
                min_cores = self._cores.exact
            else:
                min_cores = self._cores.range[0]

        if self.has_nodes:
            if self._nodes.is_exact():
                min_cores = min_cores * self._nodes.exact
            else:
                min_cores = min_cores * self._nodes.range[0]

        return min_cores

    def to_dict(self):
        """Serialize ``resource`` element of job description to dictionary

        Returns:
            dict(str): dictionary with ``resources`` element of job description
        """
        result = {}
        if self.has_cores:
            result['numCores'] = self._cores.to_dict()

        if self.has_nodes:
            result['numNodes'] = self._nodes.to_dict()

        if self.has_crs:
            result['nodeCrs'] = {crtype.name: value for (crtype, value) in self._crs.items()}

        return result

    def to_json(self):
        """Serialize ``resource`` element of job description to JSON description.

        Returns:
            JSON description of ``resource`` element of job description.
        """
        return json.dumps(self.to_dict())


class JobDependencies:
    """Runtime dependencies of job."""

    @staticmethod
    def _validate_job_list(job_list, err_msg):
        """Validate list of job names.

        Validate if given argument is a list of strings.

        Args:
            job_list (list(str)): job names list
            err_msg (str): error message to be places in IllegalJobDescription

        Raises:
            IllegalJobDescription: if ``job_list`` is not a list of strings
        """
        if not isinstance(job_list, list):
            raise IllegalJobDescription(err_msg)

        for jobname in job_list:
            if not isinstance(jobname, str):
                raise IllegalJobDescription(err_msg)

    def __init__(self, after=None):
        """Initialize runtime dependencies of a job.

        Args:
            after - list of jobs that must finish before job can be started

        Raises:
            IllegalJobDescription: when list of jobs has a wrong format.
        """
        self.after = []

        if after is not None:
            JobDependencies._validate_job_list(after, "Dependency task's list must be an array of job names")
            self.after = after

    @property
    def has_dependencies(self):
        """bool: true if job contains runtime dependencies"""
        return len(self.after) > 0

    def to_dict(self):
        """Serialize job's runtime dependencies

        Returns:
            dict(str): dictionary with job's runtime dependencies
        """
        return self.__dict__

    def to_json(self):
        """Serialize job's runtime dependencies to JSON description.

        Returns:
            JSON description of job's runtime dependencies
        """
        return json.dumps(self.to_dict())


class JobIteration:
    """The ``iteration`` element of job description."""

    def __init__(self, start=None, stop=None):
        """Initialize ``iteration`` element of job description.

        If ``start`` is not defined, the value 0 is assumed.

        Args:
            start (int): starting index of an iteration
            stop (int): stop index of an iteration - the last value of job's iteration will be ``stop`` - 1

        Raises:
            IllegalJobDescription: raised when:
                * ``stop`` is not defined
                * ``start`` is greater or equal ``stop``
        """
        if stop is None:
            raise IllegalJobDescription("Missing stop iteration value")

        if start is None:
            start = 0

        if start >= stop:
            raise IllegalJobDescription("Job iteration stop greater or equal than start")

        self.start = start
        self.stop = stop

    def in_range(self, index):
        """Check if given index is in range of job's iterations.

        Args:
            index (int): index to check

        Returns:
            bool: true if index is in range
        """
        return self.stop > index >= self.start

    def iterations_gen(self):
        """Iterations generator.

        Returns:
            int: the iteration indexes
        """
        return range(self.start, self.stop)

    def iterations(self):
        """Return number of iterations of a job.

        Returns:
            int: number of iterations
        """
        return self.stop - self.start

    def to_dict(self):
        """Serialize ``iteration`` element of job description

        Returns:
            dict(str): dictionary with ``iteration`` element of job description
        """
        return self.__dict__

    def to_json(self):
        """Serialize ``iteration`` element of job description to JSON description.

        Returns:
            JSON description of ``iteration`` element of job description
        """
        return json.dumps(self.to_dict())

    def __str__(self):
        """Return string representation of ``iteration`` element of job description.

        Returns:
            str: string representation of ``iteration`` element of job description
        """
        return "{}-{}".format(self.start, self.stop)


class SubJobState:
    """Represent state of execution of single job's iteration."""

    def __init__(self):
        """Initialize state of execution of single job's iteration.

        The initial state is set to QUEUED and all other attributes are initialized as empty elements.
        """
        self._state = JobState.QUEUED
        self._history = []
        self._messages = None
        self._runtime = {}

    def state(self):
        """Return current status of job's iteration.

        Returns:
            JobState: current status of job's iteration
        """
        return self._state

    def set_state(self, state, err_msg=None):
        """Set current state of job's iteration.

        Args:
            state (JobState): the new state of job's iteration
            err_msg (str, optional): the error message to record
        """
        assert isinstance(state, JobState), "Wrong state type"

        self._state = state
        self._history.append((state, datetime.now()))
        if err_msg:
            self.append_message(err_msg)

    def append_runtime(self, data):
        """Record job's iteration runtime statistics.

        Args:
            data (dict): the data to append to job's iteration runtime statistics
        """
        self._runtime.update(data)

    def history(self):
        """Return job's iteration state change history.

        Returns:
            list(JobState, DateTime): job's iteration state change history.
        """
        return self._history

    def messages(self):
        """Return job's iteration recorded error messages.

        Returns:
            list(str): recorded job's iteration error messages.
        """
        return self._messages

    def runtime(self):
        """Return job's iteration runtime statistics.

        Returns:
            dict: job's iteration runtime statistics
        """
        return self._runtime

    def append_message(self, msg):
        """Record job's iteration error message.

        Args:
            msg (str): error message to record
        """
        if self._messages is None:
            self._messages = msg
        else:
            self._messages = '\n'.join([self._messages, msg])


class Job:
    """Job description and state.

    Attributes:
        _name (str): job name
        _execution (JobExecution): execution description
        _resources (JobResources): resources description
        _iteration (JobIteration): iteration description
        dependencies (JobDependencies): runtime dependencies description
        attributes (dict): additional attributes
        _subjobs (list(SubJobState)): list of job's iteration states - only if ``iteration`` element defined, elements
            at positions 'job iteration' - 'iteration start'
        _subjobs_not_finished (int): number of not finished already iterations - only if ``iteration`` element defined
        _subjobs_failed (int): number of already failed iterations - only if ``iteration`` element defined
        _history (list(JobState, DateTime)): state change history
        _state (JobState): current state
        _messages: recorded error messages
        _runtime (dict): runtime information
        _queue_pos: current job's position in scheduling queue
     """

    @staticmethod
    def validate_jobname(jobname):
        """Check if given name is valid for job's name.

        Args:
            jobname (str): name to validate

        Returns:
            bool: true if name is valid
        """
        return ':' not in jobname

    def __init__(self, name, execution, resources, iteration=None, dependencies=None, attributes=None):
        """Initialize job.

        Args:
            name (str): job name
            execution (JobExecution or dict): ``execution`` element of job's description
            resources (JobResources or dict): ``resources`` element of job's description
            iteration (JobIteration or dict, optional): ``iteration`` element of job's description
            dependencies (JobDependencies or dict, optional): ``dependencies`` element of job's description
            attributes (dict, optional): additional job's attributes used by partition manager

        Raises:
            IllegalJobDescription: raised in case of wrong elements of job's description
        """
        if name is None:
            raise IllegalJobDescription("Job name not defined")

        if not Job.validate_jobname(name):
            raise IllegalJobDescription("Invalid job name {}".format(name))

        self._name = name

        if isinstance(execution, JobExecution):
            self._execution = execution
        elif isinstance(execution, dict):
            self._execution = JobExecution(**execution)
        else:
            raise IllegalJobDescription("Job execution not defined or wrong type")

        if isinstance(resources, JobResources):
            self._resources = resources
        elif isinstance(resources, dict):
            self._resources = JobResources(**resources)
        else:
            raise IllegalJobDescription("Job resources not defined or wrong type")

        if isinstance(iteration, JobIteration) or iteration is None:
            self._iteration = iteration
        elif isinstance(iteration, dict):
            try:
                self._iteration = JobIteration(**iteration)
            except IllegalJobDescription:
                raise
            except Exception:
                raise IllegalJobDescription("Job iteration wrong specification")
        else:
            raise IllegalJobDescription("Job iteration wrong type")

        if isinstance(dependencies, JobDependencies) or dependencies is None:
            self.dependencies = dependencies
        elif isinstance(dependencies, dict):
            try:
                self.dependencies = JobDependencies(**dependencies)
            except IllegalJobDescription:
                raise
            except Exception:
                raise IllegalJobDescription("Job dependencies wrong specification")
        else:
            raise IllegalJobDescription("Job dependencies wrong type")

        if attributes is not None and not isinstance(attributes, dict):
            raise IllegalJobDescription("Job attributes must be dictionary")
        self.attributes = attributes

        if self._iteration:
            self._subjobs = [SubJobState() for i in range(self._iteration.start, self._iteration.stop)]
            self._subjobs_not_finished = self._iteration.iterations()
            self._subjobs_failed = 0
        self._history = []

        self._runtime = {}

        self.canceled = False

        # history must be initialized before
        self._state = None
        self.set_state(JobState.QUEUED)

        self._messages = None

        # position in scheduling queue - None if not set
        self._queue_pos = None

    @property
    def name(self):
        """str: job's name"""
        return self._name

    def get_name(self, iteration=None):
        """Return job's or job's iteration name.

        Args:
            iteration (int, optional): if defined the iteration's name is returned

        Returns:
            str: job's or job's iteration's name
        """
        return self._name if iteration is None else '{}:{}'.format(self._name, iteration)

    @property
    def execution(self):
        """JobExecution: the ``execution`` element of job description"""
        return self._execution

    @property
    def resources(self):
        """JobExecution: the ``resources`` element of job description"""
        return self._resources

    def get_not_finished_iterations(self):
        """Return number of currently not finished iterations.

        This method is valid only for iteration jobs.

        Returns:
            int: number of not finished iterations
        """
        return self._subjobs_not_finished

    def get_failed_iterations(self):
        """Return number of already failed iterations.

        This method is valid only for iteration jobs.

        Returns:
            int: number of failed iterations
        """
        return self._subjobs_failed

    def history(self, iteration=None):
        """Return job's or job's iteration state change history.

        Args:
            iteration (int, optional): if defined the iteration's state change history is returned

        Returns:
            list(JobState, DateTime): job's or job's iteration state change history.
        """
        if iteration is None:
            return self._history

        return self._get_subjob(iteration).history()

    def messages(self, iteration=None):
        """Return job's or job's iteration recorded error messages.

        Args:
            iteration (int, optional): if defined the iteration's recorded error messages is returned

        Returns:
            list(str): recorded job's or job's iteration error messages.
        """
        if iteration is None:
            return self._messages

        return self._get_subjob(iteration).messages()

    def runtime(self, iteration=None):
        """Return job's or job's iteration runtime statistics.

        Args:
            iteration (int, optional): if defined the iteration's runtime statistics is returned

        Returns:
            dict: job's or job's iteration runtime statistics
        """
        if iteration is None:
            return self._runtime

        return self._get_subjob(iteration).runtime()

    @property
    def has_iterations(self):
        """bool: true if job has iterations"""
        return self._iteration is not None

    @property
    def iteration(self):
        """JobIteration: ``iteration`` element of job description"""
        return self._iteration

    def state(self, iteration=None):
        """Return job's or job's iteration current state.

        Args:
           iteration (int, optional): if defined the iteration's state is returned

        Returns:
           JobState: job's or job's iteration current state
        """
        if iteration is None:
            return self._state

        return self._get_subjob(iteration).state()

    def str_state(self, iteration=None):
        """Return job's or job's iteration current state as string.

        Args:
           iteration (int, optional): if defined the iteration's state is returned

        Returns:
           JobState: job's or job's iteration current state as string
        """
        return self.state(iteration).name

    def _get_subjob(self, iteration):
        """Return job's iteration state object for given iteration.

        Args:
            iteration (int): the iteration index

        Returns:
            SubJobState: job's iteration state object
        """
        return self._subjobs[iteration - self._iteration.start]

    @property
    def iteration_states(self):
        """list(SubJobState): list of iteration states"""
        return self._subjobs

    def set_state(self, state, iteration=None, err_msg=None):
        """Set current job's or job's iteration state.

        Args:
            state (JobState): new job's or job's iteration state
            iteration (int, optional): job's iteration index if the iteration state should be set
            err_msg (str, optional): the optional error message to record

        Returns:
            JobState: the job's finish state if job's iteration status change triggered job status change (for example
                the last iteration job finished, so the whole job also finished), or None if job's as a whole still
                not finished
        """
        assert isinstance(state, JobState), "Wrong state type"

        _logger.debug(f'job {self._name} iteration {iteration} status changed to {state.name} '
                      f'(final ? {state.is_finished()})')

        if iteration is not None:
            self._get_subjob(iteration).set_state(state, err_msg)

            if state.is_finished():
                self._subjobs_not_finished = self._subjobs_not_finished - 1

                if state in [JobState.FAILED, JobState.OMITTED, JobState.CANCELED]:
                    self._subjobs_failed += 1

                _logger.debug(f'for job {self._name} currently not finished subjobs {self._subjobs_not_finished}, '
                              f'failed {self._subjobs_failed}')

                if self._subjobs_not_finished == 0 and not self._state.is_finished():
                    # all subjobs finished - change whole job state
                    if self.canceled:
                        final_state = JobState.CANCELED
                    else:
                        final_state = JobState.SUCCEED if self._subjobs_failed == 0 else JobState.FAILED
                    self.set_state(final_state)
                    return final_state
        else:
            self._state = state
            self._history.append((state, datetime.now()))
            if err_msg:
                self.append_message(err_msg)

        return None

    @property
    def has_dependencies(self):
        """bool: true if job has runtime dependencies"""
        return self.dependencies is not None and self.dependencies.has_dependencies

    def append_message(self, msg):
        """Record job's error message.

        Args:
            msg (str): error message to record
        """
        if self._messages is None:
            self._messages = msg
        else:
            self._messages = '\n'.join([self._messages, msg])

    def queue_pos(self):
        """Return current position of a job in scheduling queue

        Returns:
            int: current position of a job in scheduling queue
        """
        return self._queue_pos

    def set_queue_pos(self, pos):
        """Set current position of a job in scheduling queue.

        Args:
            pos (int): current position of a job in scheduling queue
        """
        self._queue_pos = pos

    def clear_queue_pos(self):
        """Reset current position of a job in scheduling queue."""
        self._queue_pos = None

    def append_runtime(self, data, iteration):
        """Record job's or job's iteration runtime statistics.

        Args:
            data (dict): the data to append to job's or job's iteration runtime statistics
            iteration (int, optional): if defined the iteration's runtime statistics will be updated

        Args:
            data (dict): the data to append to job's or job's iteration runtime statistics
        """
        if iteration is not None:
            self._get_subjob(iteration).append_runtime(data)
        else:
            self._runtime.update(data)

    def to_dict(self):
        """Serialize job's description to dictionary.

        Returns:
            dict(str): dictionary with job description
        """
        result = {
            'name': self._name,
            'execution': self._execution.to_dict(),
            'resources': self._resources.to_dict()}

        if self._iteration is not None:
            result['iteration'] = self._iteration.to_dict()

        if self.dependencies is not None:
            result['dependencies'] = self.dependencies.to_dict()

        if self.attributes is not None:
            result['attributes'] = self.attributes

        return result

    def to_json(self):
        """Serialize job description to JSON format.

        Returns:
            JSON of job's description
        """
        return json.dumps(self.to_dict())


class JobList:
    """The list of all submited jobs.

    Attributes:
        _jmap (dict(str,Job)): dictionary with all submited jobs with name as key
    """

    def __init__(self):
        """Initialize the list."""
        self._jmap = {}

    @staticmethod
    def parse_jobname(jobname):
        """Split given name into job name and iteration.

        Args:
            jobname (str): the name to parse

        Returns:
            name, iteration: tuple with job name and iteration, if given job name didn't contain iteration index, the
              second value will be None
        """
        parts = jobname.split(':', 1)
        return parts[0], int(parts[1]) if len(parts) > 1 else None

    def add(self, job):
        """Add a new job.

        Args:
            job (Job): job to add to the list

        Raises:
            JobAlreadyExist: when job with given name already exists in list
        """
        assert isinstance(job, Job), "Wrong job type '%s'" % (type(job).__name__)

        if self.exist(job.get_name()):
            raise JobAlreadyExist(job.get_name())

        self._jmap[job.get_name()] = job

    def exist(self, jobname):
        """Check if job with given name is in list.

        Args:
            jobname (str): job name to check

        Returns:
            bool: true if job with given name is already in list
        """
        return jobname in self._jmap

    def get(self, jobname):
        """Return job with given name.

        Args:
            jobname (str): job name

        Returns:
            Job: job from the list or None if not such job has been added.
        """
        return self._jmap.get(jobname, None)

    def jobs(self):
        """Return all job names in the list.

        Returns:
            set-like object with job names
        """
        return self._jmap.keys()

    def remove(self, jobname):
        """Remove job with given name from list.

        Args:
            jobname (str): job's name to remove from list
        """
        del self._jmap[jobname]
