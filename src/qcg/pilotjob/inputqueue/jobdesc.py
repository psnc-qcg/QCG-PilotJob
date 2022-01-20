import json
import re
import logging

from qcg.pilotjob.common.errors import IllegalJobDescription
from qcg.pilotjob.common.resources import CRType
from datetime import timedelta


class JobJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.to_dict()


class JobSerializer:

    def to_dict(self):
        """Serialize object to dictionary

        Returns:
            dict(str): dictionary with object attributes
        """
        return {name: value.to_dict() if isinstance(value, JobSerializer) else value\
                for name, value in self.__dict__.items()}

    def to_json(self, **args):
        """Serialize object to JSON description.

        Returns:
            JSON description of object.
        """
        return json.dumps(self.to_dict(), **args)


class JobExecution(JobSerializer):
    """The execution element of job description.

    Attributes:
        exec (str, optional): path to the executable
        script (str, optional): bash commands to execute
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
            script (str, optional): bash commands to execute
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
        self.exec = exec
        self.script = script
        self.args = args
        self.env = env
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.modules = modules
        self.venv = venv
        self.wd = wd
        self.model = model

    def validate(self):
        """Validate data correctness.

        Raises:
            IllegalJobDescription: when element is not valid
        """
        if all((not self.exec, not self.script)):
            raise IllegalJobDescription("Job execution (exec or script) not defined")

        if self.script and (self.exec or self.args or self.env):
            raise IllegalJobDescription("Job script and exec or args or env defined")

        if self.args is not None:
            if not isinstance(self.args, list):
                raise IllegalJobDescription("Execution arguments must be an array")

        if self.env is not None:
            if not isinstance(self.env, dict):
                raise IllegalJobDescription("Execution environment must be an dictionary")
            self.env = self.env


class ResourceSize(JobSerializer):
    """The resources size element used in job description when specified the number of required cores or nodes."""

    def __init__(self, exact=None, min=None, max=None, scheduler=None):
        """Initialize resource size.

        Args:
            exact (int, optional): exact number of resources
            min (int, optional): minimum number of resources
            max (int, optional): maximum number of resources
            scheduler (dict, optional): the iteration resources scheduler, the ``name`` and ``params`` (optional) keys
        """
        self.exact = exact
        self.min = min
        self.max = max
        self.scheduler = scheduler

    @property
    def range(self):
        """(int, int): tuple with resources range"""
        return self.min, self.max

    def is_exact(self):
        """Check if resource size is defined as exact number.

        Returns:
            True: if resource size is defined as exact number
            False: if reosurce size is defined as range
        """
        return self.exact is not None

    def validate(self):
        """Validate data correctness.

        Raises:
            IllegalJobDescription: when element is not valid
        """
        if self.exact is not None and (self.min is not None or self.max is not None or self.scheduler is not None):
            raise IllegalJobDescription("Exact number of resources defined with min/max/scheduler")

        if self.max is not None and self.min is not None and self.min > self.max:
            raise IllegalJobDescription("Maximum number greater than minimal")

        if self.exact is None and self.min is None and self.max is None:
            raise IllegalJobDescription("No resources defined")

        if (self.exact is not None and self.exact < 0) or\
                (self.min is not None and self.min < 0) or\
                (self.max is not None and self.max < 0):
            raise IllegalJobDescription("Negative number of resources")


class JobResources(JobSerializer):
    """The ```resources``` element of job description."""

    _wt_regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

    def __init__(self, cores=None, nodes=None, wt=None, crs=None):
        """Initialize ``resources`` element of job description.

        * if nodes > 1, then numCores relates to each of the node, so total number of
                required cores will be a product of `nodes` and `cores`
        * crs relates to each node available consumable resources

        Args:
            cores - number of cores, either as exact number or as a range
            nodes - number of nodes, either as exact number of as a range
            wt - wall time
            crs (dict(string,int)) - each node consumable resources
        """
        self.cores = ResourceSize(**cores) if cores else None
        self.nodes = ResourceSize(**nodes) if nodes else None
        self.wt = timedelta(**wt) if wt else None
        self.crs = None

        if crs:
            self.crs = {}
            try:
                for name, count in crs.items():
                    self.crs[CRType[name.upper()]] = count
            except AttributeError:
                raise IllegalJobDescription(f'Wrong definition of consumable resources - must be a dictionary')
            except KeyError:
                raise IllegalJobDescription(f'Unknown consumable resource {name}')

    @staticmethod
    def get_wt_from_str(wt_str):
        """Parse wall time description into timedelta structure.

        Args:
            wt_str (str): the wall time description as a string

        Returns:
            timedelta: parsed wall time description

        Raises:
            IllegalResourceRequirements: when wall time description has wrong format.
        """
        parts = JobResources._wt_regex.match(wt_str)
        if not parts:
            raise IllegalJobDescription("Wrong wall time format")

        try:
            parts = parts.groupdict()
            time_params = {}
            for name, param in parts.items():
                if param:
                    time_params[name] = int(param)

            td = timedelta(**time_params)
            if td.total_seconds() == 0:
                raise IllegalJobDescription("Wall time must be greater than 0")

            return td
        except IllegalJobDescription:
            raise
        except Exception:
            raise IllegalJobDescription("Wrong wall time format")

    @property
    def has_nodes(self):
        """bool: true if ``resources`` element of job description contains number of nodes definition"""
        return self.nodes is not None

    @property
    def has_cores(self):
        """bool: true if ``resources`` element of job description contains number of cores definition"""
        return self.cores is not None

    @property
    def has_crs(self):
        """bool: true if ``resources`` element of job description contains consumable resources definition"""
        return self.crs is not None

    def get_min_num_cores(self):
        """Return minimum number of cores the job can be run.

        Returns:
            int: minimum number of required cores for the job.
        """
        min_cores = 1
        if self.cores:
            if self.cores.is_exact():
                min_cores = self.cores.exact
            else:
                min_cores = self.cores.range[0]

        if self.nodes:
            if self.nodes.is_exact():
                min_cores = min_cores * self.nodes.exact
            else:
                min_cores = min_cores * self.nodes.range[0]

        return min_cores

    def get_min_node_num_cores(self):
        """Return minimum number of cores on the single node the job can be run.

        Returns:
            int: minimum number of required cores for the job per single node.
        """
        min_cores = 1
        if self.cores:
            if self.cores.is_exact():
                min_cores = self.cores.exact
            else:
                min_cores = self.cores.range[0]

        if not self.nodes:
            # in case where number of nodes is not defined job can be distributed among any number of nodes
            min_cores = 1

        return min_cores

    def get_min_nodes(self):
        """Return minimum number of nodes job requires.

        Returns:
            int: minimum number of required nodes for the job.
        """
        min_nodes = 1
        if self.nodes:
            if self.nodes.is_exact():
                min_nodes = self.nodes.exact
            else:
                min_nodes = self.nodes.range[0]

        return min_nodes

    def validate(self):
        """Validate data correctness.

        Raises:
            IllegalJobDescription: when element is not valid
        """
        if self.cores is None and self.nodes is None:
            raise IllegalJobDescription("No resources defined")

        if self.cores is not None and not isinstance(self.cores, ResourceSize):
            raise IllegalJobDescription(f"Wrong definition of number of cores ({type(self.cores).__name__})")

        if self.nodes is not None and not isinstance(self.nodes, ResourceSize):
            raise IllegalJobDescription(f"Wrong definition of number of nodes ({type(self.nodes).__name__})")

        if self.cores:
            self.cores.validate()

        if self.nodes:
            self.nodes.validate()

        if self.crs:
            for cr_type, cr_count in self.crs.items():
                if cr_count < 1:
                    raise IllegalJobDescription(f'Number of consumable resources {cr_type} must be greater than 0')


class JobDependencies(JobSerializer):
    """Runtime dependencies of job."""

    def __init__(self, after=None):
        """Initialize runtime dependencies of a job.

        Args:
            after - list of jobs that must finish before job can be started

        Raises:
            IllegalJobDescription: when list of jobs has a wrong format.
        """
        self.after = after

    @property
    def has_dependencies(self):
        """bool: true if job contains runtime dependencies"""
        return len(self.after) > 0

    def validate(self):
        """Validate data correctness.

        Raises:
            IllegalJobDescription: when element is not valid
        """
        if not isinstance(self.after, list) or not all(isinstance(jname, str) for jname in self.after):
            raise IllegalJobDescription('Dependency task list must be an array of strings')


class JobIteration(JobSerializer):
    """The ``iteration`` element of job description."""

    def __init__(self, start=None, stop=None):
        """Initialize ``iteration`` element of job description.

        Args:
            start (int): starting index of an iteration
            stop (int): stop index of an iteration - the last value of job's iteration will be ``stop`` - 1
        """
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

    def iterations(self):
        """Return number of iterations of a job.

        Returns:
            int: number of iterations
        """
        return self.stop - self.start

    def normalize(self, iteration):
        """Return `normalized` index of iteration.
        The normalized index is in range (0, total_iterations).

        Args:
            iteration (int): index

        Returns:
            the normalized value of index from range (0, total_iterations)
        """
        return iteration - self.start

    def validate(self):
        """Validate data correctness.

        Raises:
            IllegalJobDescription: when element is not valid
        """
        if self.stop is None:
            raise IllegalJobDescription("Missing stop iteration value")

        if self.start is None:
            raise IllegalJobDescription("Missing start iteration value")

        if self.start >= self.stop:
            raise IllegalJobDescription("Job iteration stop greater or equal than start")

    def __str__(self):
        """Return string representation of ``iteration`` element of job description.

        Returns:
            str: string representation of ``iteration`` element of job description
        """
        return "{}-{}".format(self.start, self.stop)


class JobDescription(JobSerializer):

    def __init__(self, name, execution, resources=None, dependencies=None, iteration=None):
        self.name = name

        try:
            self.execution = JobExecution(**execution)
        except Exception as exc:
            raise IllegalJobDescription(f'illformed execution element: {str(exc)}')

        try:
            self.resources = JobResources(**resources) if resources else None
        except:
            raise IllegalJobDescription('illformed resource element')

        try:
            self.dependencies = JobDependencies(**dependencies) if dependencies else None
        except:
            raise IllegalJobDescription('illformed dependencies element')

        try:
            self.iteration = JobIteration(**iteration) if iteration else None
        except:
            raise IllegalJobDescription('illformed iteration element')

    def validate(self):
        """Validate data correctness.

        Raises:
            IllegalJobDescription: when element is not valid
        """
        if not self.name or any(c in self.name for c in ['$', '{', '}', '(', ')', '\'', '"', ' ', '\t', '\n', ':']):
            raise IllegalJobDescription('Missing or invalid content of job name')

        self.execution.validate()

        if self.resources:
            self.resources.validate()

        if self.dependencies:
            self.dependencies.validate()

        if self.iteration:
            self.iteration.validate()
