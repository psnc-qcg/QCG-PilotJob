import json
import asyncio
import os
import logging
from string import Template

from qcg.pilotjob.inputqueue.jobdesc import JobExecution
from qcg.pilotjob.executor.launcher.environment import CommonEnvironment, SlurmEnvironment
from qcg.pilotjob.executor.launcher.slurmjobmodels import JobModels
from qcg.pilotjob.executor.slurmres import in_slurm_allocation
from qcg.pilotjob.executor.launcher.events import ExAgentEvent

from cached_property import cached_property

_logger = logging.getLogger(__name__)


class ProcessLauncher:

    def __init__(self, executing_iteration):
        self.executing_iteration = executing_iteration

    @staticmethod
    def get_launcher(executing_iteration):
        if executing_iteration.allocation.cores > 1 and in_slurm_allocation():
            return SlurmProcessLauncher(executing_iteration)
        else:
            return DirectProcessLauncher(executing_iteration)

    def get_job_variables(self):
        return {
            'ncores': self.executing_iteration.allocation.cores,
            'nnodes': len(self.executing_iteration.allocation.nodes),
            'nlist': ','.join([node['name'] for node in self.executing_iteration.allocation.nodes]),
            'it': self.executing_iteration.iteration
        }

    @cached_property
    def job_execution(self):
        job_exec = JobExecution(
            **json.loads(Template(self.executing_iteration.description.execution.to_json()).safe_substitute(
                self.get_job_variables())))

        if job_exec.modules is not None or job_exec.venv is not None:
            job_exec = job_exec.exec
            job_args = job_exec.args

            job_exec.exec = 'bash'

            bash_cmd = ''
            if job_exec.modules:
                bash_cmd += f'source /etc/profile && module load {" ".join(job_exec.modules)}; '

            if job_exec.venv:
                bash_cmd += 'source {}/bin/activate;'.format(job_exec.venv)

            if job_exec.script:
                bash_cmd += job_exec.script
            else:
                bash_cmd += 'exec {} {}'.format(
                    job_exec, ' '.join([str(arg).replace(" ", "\\ ") for arg in job_args]))

            job_exec.args = ['-l', '-c', bash_cmd]
        else:
            if job_exec.script:
                job_exec.exec = 'bash'
                job_exec.args = ['-l', '-c', job_exec.script]

        return job_exec

    @cached_property
    def env(self):
        job_execution = self.job_execution;

        if job_execution.env:
            job_env = job_execution.env.copy()
        else:
            job_env = {}

        CommonEnvironment.update_env(self.executing_iteration, job_env)

        return job_env

    @cached_property
    def workdir(self):
        job_wd = self.job_execution.wd
        if job_wd:
            job_wd = os.path.join(os.getcwd(), job_wd)
        else:
            job_wd = os.getcwd()

        return job_wd

    def setup_sandbox(self):
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)


class SlurmProcessLauncher(ProcessLauncher):

    def __init__(self, executing_iteration):
        super().__init__(executing_iteration)

    async def prepare(self):
        self.setup_sandbox()

    async def launch(self):
        try:
            job_model = self.job_execution.model or 'default'
            preprocess_method = JobModels().get_model(job_model)

            env = self.env
            SlurmEnvironment.update_env(self.executing_iteration, env)

            job_execution = self.job_execution
            stdin_p = None
            stdout_p = asyncio.subprocess.DEVNULL
            stderr_p = asyncio.subprocess.DEVNULL

            preprocess_method(self.executing_iteration.id, job_execution, self.executing_iteration.allocation, env,
                              self.workdir)

            app_exec = job_execution.exec
            app_args = job_execution.args

            if job_execution.stdin:
                stdin_path = job_execution.stdin if os.path.isabs(job_execution.stdin) else os.path.join(self.workdir, job_execution.stdin)
                stdin_p = open(stdin_path, 'r')

            if job_execution.stdout and job_execution.stderr and job_execution.stdout == job_execution.stderr:
                stdout_path = job_execution.stdout if os.path.isabs(job_execution.stdout) else os.path.join(self.workdir, job_execution.stdout)
                stdout_p = stderr_p = open(stdout_path, 'w')
            else:
                if job_execution.stdout:
                    stdout_path = job_execution.stdout if os.path.isabs(job_execution.stdout) else os.path.join(self.workdir,
                                                                                                job_execution.stdout)
                    stdout_p = open(stdout_path, 'w')

                if job_execution.stderr:
                    stderr_path = job_execution.stderr if os.path.isabs(job_execution.stderr) else os.path.join(self.workdir,
                                                                                                job_execution.stderr)
                    stderr_p = open(stderr_path, 'w')

            _logger.debug(f'launching iteration {self.executing_iteration.id}: {app_exec} {app_args}')

            process = await asyncio.create_subprocess_exec(
                app_exec, *app_args if app_args else [],
                stdin=stdin_p,
                stdout=stdout_p,
                stderr=stderr_p,
                cwd=self.workdir,
                env=env,
                shell=False,
            )
            ExAgentEvent.process_launch({'iteration': self.executing_iteration.id, 'pid': process.pid,
                                         'method': 'slurm', 'dir': self.workdir }).publish()

            _logger.info(f'iteration {self.executing_iteration.id} local process launched with pid {process.pid}')

            await process.wait()

            self.executing_iteration.exit_code = process.returncode

            ExAgentEvent.process_finish({'iteration': self.executing_iteration.id, 'pid': process.pid,
                                         'method': 'slurm', 'exit_code': process.returncode }).publish()

            _logger.info(f'iteration {self.executing_iteration.id} process finished finished with '
                         f'{process.returncode} exit code')
        except Exception as exc:
            self.executing_iteration.exit_code = -1
            self.executing_iteration.message = str(exc)
            _logger.exception(f'iteration {self.executing_iteration.id} failed: {str(exc)}')
        finally:
            try:
                if stdin_p:
                    stdin_p.close()
                if stdout_p != asyncio.subprocess.DEVNULL:
                    stdout_p.close()
                if stderr_p not in [asyncio.subprocess.DEVNULL, stdout_p]:
                    stderr_p.close()
            except Exception as exc:
                _logger.error(f'iteration {self.executing_iteration.id} cleanup failed: {str(exc)}')

class DirectProcessLauncher(ProcessLauncher):

    def __init__(self, executing_iteration):
        super().__init__(executing_iteration)

    async def prepare(self):
        self.setup_sandbox()

    async def launch(self):
        try:
            jexec = self.job_execution
            stdin_p = None
            stdout_p = asyncio.subprocess.DEVNULL
            stderr_p = asyncio.subprocess.DEVNULL

            _logger.info(f'job execution before launch: {jexec.to_dict()}')
            if jexec.stdin:
                stdin_path = jexec.stdin if os.path.isabs(jexec.stdin) else os.path.join(self.workdir, jexec.stdin)
                stdin_p = open(stdin_path, 'r')

            if jexec.stdout and jexec.stderr and jexec.stdout == jexec.stderr:
                stdout_path = jexec.stdout if os.path.isabs(jexec.stdout) else os.path.join(self.workdir, jexec.stdout)
                stdout_p = stderr_p = open(stdout_path, 'w')
            else:
                if jexec.stdout:
                    stdout_path = jexec.stdout if os.path.isabs(jexec.stdout) else os.path.join(self.workdir,
                                                                                                jexec.stdout)
                    stdout_p = open(stdout_path, 'w')

                if jexec.stderr:
                    stderr_path = jexec.stderr if os.path.isabs(jexec.stderr) else os.path.join(self.workdir,
                                                                                                jexec.stderr)
                    stderr_p = open(stderr_path, 'w')

            allocation = self.executing_iteration.allocation

            if allocation.binding:
                app_exec = 'taskset'
                app_args = ['-c', ','.join([str(c) for c in allocation.nodes[0]['cores']]), jexec.exec]
                if jexec.args:
                    app_args.extend(jexec.args)
            else:
                app_exec = jexec.exec
                app_args = jexec.args or []

            _logger.debug(f'launching iteration {self.executing_iteration.id}: {app_exec} {app_args}')

            process = await asyncio.create_subprocess_exec(
                app_exec, *app_args,
                stdin=stdin_p,
                stdout=stdout_p,
                stderr=stderr_p,
                cwd=self.workdir,
                env=self.env,
                shell=False,
            )

            ExAgentEvent.process_launch({'iteration': self.executing_iteration.id, 'pid': process.pid,
                                         'method': 'direct', 'dir': self.workdir }).publish()

            _logger.info(f'iteration {self.executing_iteration.id} local process launched with pid {process.pid}')

            await process.wait()

            self.executing_iteration.exit_code = process.returncode

            ExAgentEvent.process_finish({'iteration': self.executing_iteration.id, 'pid': process.pid,
                                         'method': 'direct', 'exit_code': process.returncode }).publish()

            _logger.info(f'iteration {self.executing_iteration.id} process finished finished with '
                         f'{process.returncode} exit code')
        except Exception as exc:
            self.executing_iteration.exit_code = -1
            self.executing_iteration.message = str(exc)
            _logger.exception(f'iteration {self.executing_iteration.id} failed: {str(exc)}')
        finally:
            try:
                if stdin_p:
                    stdin_p.close()
                if stdout_p != asyncio.subprocess.DEVNULL:
                    stdout_p.close()
                if stderr_p not in [asyncio.subprocess.DEVNULL, stdout_p]:
                    stderr_p.close()
            except Exception as exc:
                _logger.error(f'iteration {self.executing_iteration.id} cleanup failed: {str(exc)}')
