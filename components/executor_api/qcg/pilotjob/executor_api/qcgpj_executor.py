import ast
import logging
import textwrap
from concurrent.futures import Executor
from enum import Enum
from string import Template
from typing import Tuple, Any, Dict, Callable, Union

from qcg.pilotjob.api.job import Jobs
from qcg.pilotjob.api.manager import LocalManager
from qcg.pilotjob.executor_api.qcgpj_future import QCGPJFuture

_logger = logging.getLogger(__name__)


class QCGPJExecutor(Executor):
    """QCG-PilotJob Executor. It provides simplified interface for common uses of QCG-PilotJob

    Parameters
    ----------
    wd : str, optional
        Working directory where QCG-PilotJob manager should be started, by default it is
        a current directory
    resources : str, optional
        The resources to use. If specified forces usage of Local mode of QCG-PilotJob Manager.
        The format is compliant with the NODES format of QCG-PilotJob, i.e.:
        [node_name:]cores_on_node[,node_name2:cores_on_node][,...].
        Eg. to define 4 cores on an unnamed node use `resources="4"`,
        to define 2 nodes: node_1 with 2 cores and node_2 with 3 cores, use `resources="node_1:2,node_2:3"`
    reserve_core : bool, optional
        If True reserves a core for QCG-PilotJob Manager instance,
        by default QCG-PilotJob Manager shares a core with computing tasks
        Parameters.
    enable_rt_stats : bool, optional
        If True, QCG-PilotJob Manager will collect its runtime statistics
    wrapper_rt_stats : str, optional
        The path to the QCG-PilotJob Manager tasks wrapper program used for collection of statistics
    log_level : str, optional
        Logging level for QCG-PilotJob Manager (for both service and client part).
    other_args : optional
        Optional list of additional arguments for initialisation of QCG-PilotJob Manager

    Returns
    -------
    None

    """
    def __init__(self,
                 *other_args,
                 wd=".",
                 resources=None,
                 reserve_core=False,
                 enable_rt_stats=False,
                 wrapper_rt_stats=None,
                 log_level='info'
                 ):

        self.finished = False

        # ---- QCG PILOT JOB INITIALISATION ---

        # Establish logging levels
        service_log_level, client_log_level = self._setup_qcgpj_logging(log_level)

        # Prepare input arguments for QCG-PJM

        args = ['--log', service_log_level,
                '--wd', wd]

        if resources:
            args.append('--nodes')
            args.append(str(resources))

        if reserve_core:
            args.append('--system-core')

        if enable_rt_stats:
            args.append('--enable-rt-stats')

        if wrapper_rt_stats:
            args.append('--wrapper-rt-stats')
            args.append(wrapper_rt_stats)

        if other_args:
            args.extend(other_args)

        client_conf = {'log_file': wd + '/api.log', 'log_level': client_log_level}

        with open(wd + "/executor_api.log", "w") as f:
            f.write(f'Starting QCG-PJ Manager with arguments: {args}')

        # create QCGPJ Manager (service part)
        self._qcgpjm = LocalManager(args, client_conf)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()

    def shutdown(self, wait=True):
        """Shutdowns the QCG-PJ manager service. If it is already closed, the method has no effect.
        """
        if not self.finished:
            self._qcgpjm.finish()
            self.finished = True
        else:
            pass

    def submit(self, fn: Callable[..., Union[str, Tuple[str, Dict[str, Any]]]], *args, **kwargs):
        """Submits a specific task to the QCG-PJ manager using template-based, executor-like interface.

        Parameters
        ----------
        fn : Callable
            A callable that returns a tuple representing a task's template.
            The first element of the tuple should be a string containing
            a QCG-PilotJob task's description with placeholders
            (identifiers preceded by $ symbol) and the second a dictionary
            that assigns default values for selected placeholders.
        *args: variable length list with dicts, optional
            A set of dicts which contain parameters that will be used to substitute placeholders
            defined in the template.
            Note: *args overwrite defaults, but they are overwritten by **kwargs
        **kwargs: arbitrary keyword arguments
            A set of keyword arguments that will be used to substitute placeholders defined in
            the template.
            Note: **kwargs overwrite *args and defaults.

        Returns
        -------
        QCGPJFuture
            The QCGPJFuture object assigned with the submitted task

        """
        template = fn()
        if isinstance(template, tuple):
            template_str = template[0]
            defaults = template[1]
        else:
            template_str = template
            defaults = {}

        t = Template(textwrap.dedent(template_str))

        substitutions = {}

        for a in args:
            if a is not None:
                substitutions.update(a)

        substitutions.update(kwargs)

        td_str = t.substitute(defaults, **substitutions)
        td = ast.literal_eval(td_str)
        if 'env' not in td['execution']:
            td['execution']['env'] = {}
        td['execution']['env']['QCG_PM_EXEC_API_JOB_ID'] = '${jname}'
        jobs = Jobs()
        jobs.add_std(td)
        jobs_ids = self._qcgpjm.submit(jobs)
        return QCGPJFuture(jobs_ids, self._qcgpjm)

    @property
    def qcgpj_manager(self):
        """Returns current QCG-PilotJob manager instance
        """
        return self._qcgpjm

    @staticmethod
    def _setup_qcgpj_logging(log_level):
        log_level = log_level.upper()

        try:
            service_log_level = ServiceLogLevel[log_level].value
        except KeyError:
            service_log_level = ServiceLogLevel.DEBUG.value

        try:
            client_log_level = ClientLogLevel[log_level].value
        except KeyError:
            client_log_level = ClientLogLevel.DEBUG.value

        return service_log_level, client_log_level


class ServiceLogLevel(Enum):
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"


class ClientLogLevel(Enum):
    INFO = "info"
    DEBUG = "debug"
