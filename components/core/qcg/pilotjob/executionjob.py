import asyncio
import logging
import uuid
import os
import json
import psutil

from datetime import datetime
from string import Template

from qcg.pilotjob.joblist import JobExecution
from qcg.pilotjob.launcher.launcher import Launcher
from qcg.pilotjob.config import Config
import qcg.pilotjob.profile


_logger = logging.getLogger(__name__)


class ExecutionJob:
    """
    Class is responsible for executing a single job iteration inside allocation.

    Attributes:
        allocation (Allocation): job iteration allocation to execute on
        job_iteration (SchedulingIteration): job iteration data
        _envs (list(Environment)): list of environment instances which should prepare environment variables for
            execution
        id (str): unique execution identifier
        exit_code (int): job iteration exit code
        _executor (Executor): the executor instance
        error_message (str): the error description from job iteration execution
        env_opts (dict): the options for environment instances
        wd_path (str): path to the job iteration working directory
        env (dict): target job iteration environment variables
        _start_time (DateTime): moment of job iteration execution start
        _stop_time (DateTime): moment of job iteration execution stop
        nnodes (int): number of allocation nodes
        ncores (int): total number of allocation cores
        nlist (list(str)): list of allocation node names
        tasks_per_node (str): string describing number of cores (separated by comma) on each node
        job_execution (JobExecution): job execution element with variables replaced for specific iteration
        process_pid (int): launched process identifier
        process_name (str): launched process name
    """

    SIG_KILL_TIMEOUT = 10 # in seconds

    def __init__(self, executor, envs, allocation, job_iteration):
        """Initialize instance.

        Args:
            executor (Executor): object for tracing job iteration executions
            envs (list(Environment)): job iteration environments
            allocation (Allocation): scheduled allocation for job iteration
            job_iteration (SchedulingIteration) : job iteration to execute
        """
        self.allocation = allocation
        self.job_iteration = job_iteration
        self._envs = envs
        self.jid = str(uuid.uuid4().hex)
        self.exit_code = None
        self._executor = executor
        self.error_message = None
        self.env_opts = {}

        # temporary
        self.wd_path = '.'

        # inherit environment variables from parent process
        self.env = os.environ.copy()

        self._start_time = None
        self._stop_time = None

        self.canceled = False

        self.nnodes = len(self.allocation.nodes)
        self.ncores = sum([node.ncores for node in self.allocation.nodes])
        self.nlist = ','.join([node.node.name for node in self.allocation.nodes])
        self.tasks_per_node = ','.join([str(node.ncores) for node in self.allocation.nodes])

        self.job_execution = self._replace_job_variables(self.job_iteration.job.execution)

        self.process_pid = -1

        # in the following steps this exec might be modified by execution schemas
        # so in this moment this is exactly what user want to run
        if self.job_execution.script:
            self.process_name = 'bash'
        else:
            self.process_name = os.path.basename(self.job_execution.exec)

    def _replace_job_variables(self, job_execution):
        """Replace any variables used in job description.

        Args:
            job_execution (JobExecution): original job description

        Returns:
            JobExecution: job description with replaced variables
        """
        job_vars = {
            'root_wd': self._executor.base_wd,
            'ncores': str(self.ncores),
            'nnodes': str(self.nnodes),
            'nlist': self.nlist
        }

        if self.job_iteration.iteration is not None:
            job_vars['it'] = self.job_iteration.iteration
            job_vars['itval'] = self.job_iteration.iteration_value

        replaced_job_execution = JobExecution(
            **json.loads(Template(job_execution.to_json()).safe_substitute(job_vars)))

        return replaced_job_execution

    def preprocess(self):
        """Prepare environment for job execution.
        Setup sandbox and environment variables. Resolve module loading and virtual environment activation.
        """
        self._prepare_env()
        self._resolve_mods_venv()

    def setup_sandbox(self):
        """Set a job's working directory based on execution description and root working directory of executor.
        An attribute ``wd_path`` is set as an output of this method and directory is created
        """
        if self.job_execution.wd is None:
            self.wd_path = self._executor.base_wd
        else:
            self.wd_path = self.job_execution.wd

            if not os.path.isabs(self.wd_path):
                self.wd_path = os.path.join(self._executor.base_wd, self.wd_path)

        if not os.path.exists(self.wd_path):
            os.makedirs(self.wd_path)

    def _prepare_env(self):
        """Setup an execution environment.
        Mostly environment variables are set
        """
        if self.job_execution.env is not None:
            self.env.update(self.job_execution.env)

        if self._executor.zmq_address:
            self.env.update({
                'QCG_PM_ZMQ_ADDRESS': self._executor.zmq_address
            })

        if self._envs:
            for env in self._envs:
                env.update_env(self, self.env, self.env_opts)

    def _resolve_mods_venv(self):
        """When modules or virtualenv is defined in job description this function modifies job description to run
        bash with proper arguments.
        """
        # in case of modules or virtualenv we have to run bash first with proper application
        # after the modules and virtualenv activation.
        if self.job_execution.modules is not None or self.job_execution.venv is not None:
            job_exec = self.job_execution.exec
            job_args = self.job_execution.args

            self.job_execution.exec = 'bash'

            bash_cmd = ''
            if self.job_execution.modules:
                bash_cmd += f'source /etc/profile && module load {" ".join(self.job_execution.modules)}; '

            if self.job_execution.venv:
                bash_cmd += 'source {}/bin/activate;'.format(self.job_execution.venv)

            # the case when there are both elements: 'script' & 'exec/args' will occur
            # when there is 'model' - 'openmpi/srunmpi/intelmpi/threads'
            # the 'model' element sets/changes 'exec' & 'args'
            if self.job_execution.script and job_exec:
                final_args = job_args or []
                final_args.extend(['bash', '-l', '-c', f"'{self.job_execution.script}'"])
                bash_cmd += 'exec {} {}'.format(job_exec, ' '.join([str(arg) for arg in final_args]))
            elif self.job_execution.script:
                bash_cmd += self.job_execution.script
            else:
                bash_cmd += 'exec {} {}'.format(job_exec, ' '.join([str(arg) for arg in job_args]))

            self.job_execution.args = ['-l', '-c', bash_cmd]
        else:
            if self.job_execution.script:
                if self.job_execution.exec:
                    if self.job_execution.args:
                        self.job_execution.args.extend(['bash', '-l', '-c', self.job_execution.script])
                    else:
                        self.job_execution.args = ['bash', '-l', '-c', self.job_execution.script]
                else:
                    self.job_execution.exec = 'bash'
                    self.job_execution.args = ['-l', '-c', self.job_execution.script]

    def pre_start(self):
        """This method should be executed before job will be started.
        The job statistics are updated (workdir, job start time) and executor is notified about job start.
        """
        self._start_time = datetime.now()

        self._executor.job_iteration_started(self)
        self.job_iteration.job.append_runtime({'wd': self.wd_path}, iteration=self.job_iteration.iteration)

    async def launch(self):
        """The function for launching process of job iteration"""
        raise NotImplementedError('This method must be implemened in subclass')

    def postprocess(self, exit_code, error_message=None):
        """Postprocess job execution.
        Update job statistics (runtime), set the exit code and optionally error message and notify executor about job
        finish.

        Args:
            exit_code (int): job iteration exit code
            error_message (str, optional): error description
        """
        self._stop_time = datetime.now()
        if self._start_time:
            job_runtime = self._stop_time - self._start_time
        else:
            job_runtime = 0

        self.job_iteration.job.append_runtime({'rtime': str(job_runtime),
                                               'exit_code': str(exit_code),
                                               'pid': str(self.process_pid),
                                               'pname': self.process_name}, iteration=self.job_iteration.iteration)

        self.exit_code = exit_code
        self.error_message = error_message
        self._executor.job_iteration_finished(self)

    async def run(self):
        """Prepare environment for a job and launch job process."""
        try:
            self.preprocess()

            self.pre_start()

            await self.launch()
        except Exception as ex:
            _logger.exception("failed to start job %s", self.job_iteration.name)
            self.postprocess(-1, str(ex))

    async def cancel(self):
        """Cancel running job."""
        _logger.info('setting executing job canceled mode to TRUE')
        self.canceled = True


class LocalSchemaExecutionJob(ExecutionJob):
    """Run job iteration as a local process according to defined schema.

    Attributes:
        _schema (ExecutionSchmea): execution schema instance
    """

    def __init__(self, executor, envs, allocation, job_iteration, schema):
        """Initialize instance.

        Args:
            executor (Executor): executor instance
            envs (list(Environment)): list of environment instances
            allocation (Allocation): scheduled allocation for job iteration
            schema (ExecutionSchema): execution schema
        """
        super().__init__(executor, envs, allocation, job_iteration)

        self._schema = schema
        if schema:
            self.env_opts = schema.get_env_opts()

        self._process = None

    def preprocess(self):
        """Prepare environment for job execution.
        Setup environment and modify exec according to the execution schema.
        """
        super().setup_sandbox()
        self._schema.preprocess(self)
        super().preprocess()

    async def _execute_local_process(self):
        """Asynchronous task to launch local process with job iteration"""
        try:
            jexec = self.job_execution
            stdin_p = None
            stdout_p = asyncio.subprocess.DEVNULL
            stderr_p = asyncio.subprocess.DEVNULL

            if jexec.stdin:
                stdin_path = jexec.stdin if os.path.isabs(jexec.stdin) else os.path.join(self.wd_path, jexec.stdin)
                stdin_p = open(stdin_path, 'r')

            if jexec.stdout and jexec.stderr and jexec.stdout == jexec.stderr:
                stdout_path = jexec.stdout if os.path.isabs(jexec.stdout) else os.path.join(self.wd_path, jexec.stdout)
                stdout_p = stderr_p = open(stdout_path, 'w')
            else:
                if jexec.stdout:
                    stdout_path = jexec.stdout if os.path.isabs(jexec.stdout) else os.path.join(self.wd_path,
                                                                                                jexec.stdout)
                    stdout_p = open(stdout_path, 'w')

                if jexec.stderr:
                    stderr_path = jexec.stderr if os.path.isabs(jexec.stderr) else os.path.join(self.wd_path,
                                                                                                jexec.stderr)
                    stderr_p = open(stderr_path, 'w')

            _logger.debug(f"launching job {self.job_iteration.name}: {jexec.exec} {str(jexec.args)}")

            self._process = await asyncio.create_subprocess_exec(
                jexec.exec, *jexec.args,
                stdin=stdin_p,
                stdout=stdout_p,
                stderr=stderr_p,
                cwd=self.wd_path,
                env=self.env,
                shell=False,
            )

            self.process_pid = self._process.pid

            _logger.info(f"local process {self._process.pid} for job {self.job_iteration.name} launched")

            await self._process.wait()

            _logger.info(f"local process for job {self.job_iteration.name} finished")

            exit_code = self._process.returncode

            self._process = None

            self.postprocess(exit_code)
        except Exception as exc:
            _logger.exception(f"execution failed: {str(exc)}")
            self.postprocess(-1, str(exc))
        finally:
            try:
                if stdin_p:
                    stdin_p.close()
                if stdout_p != asyncio.subprocess.DEVNULL:
                    stdout_p.close()
                if stderr_p not in [asyncio.subprocess.DEVNULL, stdout_p]:
                    stderr_p.close()
            except Exception as exc:
                _logger.error(f"cleanup failed: {str(exc)}")

    async def launch(self):
        """Create asynchronous task to launch job iteration"""
        asyncio.ensure_future(self._execute_local_process())

    async def cancel(self):
        """Cancel running job."""
        _logger.info('canceling local job ...')
        await super().cancel()

        if self._process:
            # initialy send SIGTERM signal
            self._process.terminate()

        cancel_start_time = datetime.now()

        while self._process:
            # wait a moment
            await asyncio.sleep(1)

            # check if processes still exists
            if not self._process:
                _logger.debug(f'canceled process finished')
                break
            else:
                pid = self._process.pid

                try:
                    p = psutil.Process(pid)
                    _logger.debug(f'process {pid} still exists')
                    # process not finished
                except psutil.NoSuchProcess:
                    # process finished
                    pass
                    break
                except Exception as exc:
                    _logger.warning(f'failed to check process status: {str(exc)}')

                if (datetime.now() - cancel_start_time).total_seconds() > ExecutionJob.SIG_KILL_TIMEOUT:
                    # send SIGKILL signal
                    try:
                        _logger.info(f'killing {pid} process')
                        self._process.kill()
                    except Exception as exc:
                        _logger.warning(f'failed to kill process {pid}: {str(exc)}')
                    return

class LauncherExecutionJob(ExecutionJob):
    """Run job iteration with Launcher service.

    Attributes:
        env_opts (dict): options for environment instances
    """

    launcher = None

    @classmethod
    def start_agents(cls, config, wdir, aux_dir, nodes, binding, manager):
        """Start Launcher service agents on all nodes.

        Args:
            config (dict): a dictionary with configuration
            wdir (str): launcher agent working directory
            aux_dir (str): launcher agent directory for placing log and temporary files
            nodes (list(str)): list of nodes where to start launcher agents
            binding (bool): does the launcher agents should bind executed job iterations to specific cpus
            manager (manager): used to call scheduler on certain events
        """
        if cls.launcher:
            raise Exception('launcher agents already ininitialized')

        agents = [{'agent_id': node.name,
                   'node': node,
                   Config.NL_START_METHOD.get(config): {'node': node.name},
                   'options': {'binding': binding,
                               'aux_dir': aux_dir,
                               'log_level': Config.LOG_LEVEL.get(config),
                               'proc_stats': Config.ENABLE_PROC_STATS.get(config),
                               'rt_stats': Config.ENABLE_RT_STATS.get(config),
                               'rt_wrapper': Config.WRAPPER_RT_STATS.get(config) }} for node in nodes]
        cls.launcher = Launcher(config, wdir, aux_dir, manager)

        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(cls.launcher.start(agents)))

    @classmethod
    async def stop_agents(cls):
        """Stop all launcher service agents."""
        if cls.launcher:
            await cls.launcher.stop()
            cls.launcher = None

    def __init__(self, executor, envs, allocation, job_iteration, schema):
        """Initialize instance.

        Args:
            executor (Executor): executor instance
            envs (list(Environment)): list of environment instances
            allocation (Allocation): scheduled allocation for job iteration
            job_iteration (SchedulingIteration): execution schema
            schema (ExecutionSchema): execution schema
        """
        super().__init__(executor, envs, allocation, job_iteration)
        self._schema = schema
        if schema:
            self.env_opts = schema.get_env_opts()

    def preprocess(self):
        """Prepare environment for job execution.
        Setup sandbox and environment variables. Resolve module loading and virtual environment activation.
        """
        super().setup_sandbox()
        self._schema.preprocess(self)
        super().preprocess()

    async def launch(self):
        """Submit job to the specific launcher agent."""
        jexec = self.job_execution
        node = self.allocation.nodes[0]

        _logger.info(f'sending {self.job_iteration.name} to agent {node.node.name} ...')
        await self.__class__.launcher.submit(node.node.name,
                                             self.jid,
                                             self.job_iteration.name,
                                             [jexec.exec, *jexec.args],
                                             stdin=os.path.join(self.wd_path, jexec.stdin) if jexec.stdin else None,
                                             stdout=os.path.join(self.wd_path, jexec.stdout) if jexec.stdout else None,
                                             stderr=os.path.join(self.wd_path, jexec.stderr) if jexec.stderr else None,
                                             env=self.env,
                                             wdir=self.wd_path,
                                             cores=node.cores,
                                             finish_cb=self._finish_cb)

    def _finish_cb(self, message):
        """Method called when remote job iteration process finish.

        Args:
            message (dict): the finish status sent by launcher agent, the ``ec`` key is interpreted as exit code,
                and ``message`` key as error description
        """
        self.process_pid = message.get('pid', -1)

        self.postprocess(int(message.get('ec', -1)), message.get('message', None))

    async def cancel(self):
        """Cancel running job."""
        await super().cancel()

        node = self.allocation.nodes[0]
        try:
            _logger.info('canceling launcher job ...')
            await self.__class__.launcher.cancel(node.node.name, self.jid)
        except Exception as exc:
            _logger.error(f'failed to cancel application {self.jid}: {str(exc)}')
