import os
import logging

from qcg.pilotjob import logger as top_logger
from qcg.pilotjob.errors import InternalError
from qcg.pilotjob.resources import ResourcesType
from qcg.pilotjob.config import Config


_logger = logging.getLogger(__name__)


class ExecutionSchema:
    """Method of executing job.
    Currently two methods are supported:
        SlurmExecuition - jobs are run via 'srun' command
        DirectExecution - jobs are run as a normal processes

    Attributes:
        resources (Resources): available resources and their origin
        config (dict): QCG-PilotJob configuration
    """

    @classmethod
    def get_schema(cls, resources, config):
        """Create and return suitable instance of execution schema.

        Currently decision about type of execution schema is taken based on origin of resources - if QCG-PilotJob
        manager is run inside Slurm allocation, the SlurmExecution is selected. In other cases the DirectExecution
        schema is instantiated.

        Args:
            resources (Resources): available resources
            config (dict): QCG-PilotJob configuration

        Returns:
            ExecutionSchema: instance of execution schema
        """
        if resources.rtype not in __SCHEMAS__:
            raise InternalError('Unknown resources type: {}'.format(resources.rtype))

        return __SCHEMAS__[resources.rtype](resources, config)

    def __init__(self, resources, config):
        """Initialize instance.

        Args:
            resources (Resources): available resources
            config (dict): QCG-PilotJob configuration
        """
        self.resources = resources
        self.config = config

    def preprocess(self, ex_job):
        """Preprocess job iteration description before launching.
        This method might be implemented in child classes.

        Args
            ex_job (ExecutionJob): execution job iteration data
        """

    def get_env_opts(self):
        """Return options for environment instances.
        This method might be implemented in child classes"""
        return {}


class SlurmExecution(ExecutionSchema):
    """The Slurm execution schema.
    The jobs are launched with ``srun`` command.
    """

    EXEC_NAME = 'slurm'

    JOB_MODELS = {
        "threads": "_preprocess_threads",
        "intelmpi": "_preprocess_intelmpi",
        "openmpi": "_preprocess_openmpi",
        "srunmpi": "_preprocess_srunmpi",
        "default": "_preprocess_default"
    }

    def _preprocess_common(self, ex_job):
        if ex_job.job_execution.stdin:
            ex_job.job_execution.args.extend(["-i", os.path.join(ex_job.wd_path, ex_job.job_execution.stdin)])
            ex_job.job_execution.stdin = None

        if ex_job.job_execution.stdout:
            ex_job.job_execution.args.extend(["-o", os.path.join(ex_job.wd_path, ex_job.job_execution.stdout)])
            ex_job.job_execution.stdout = None

        if ex_job.job_execution.stderr:
            ex_job.job_execution.args.extend(["-e", os.path.join(ex_job.wd_path, ex_job.job_execution.stderr)])
            ex_job.job_execution.stderr = None

        if ex_job.job_iteration.resources.wt:
            ex_job.job_execution.args.extend(["--time", "0:{}".format(
                int(ex_job.job_iteration.resources.wt.total_seconds()))])

        if self.resources.binding:
            ex_job.env.update({'QCG_PM_CPU_SET': ','.join([str(c) for c in sum(
                [alloc.cores for alloc in ex_job.allocation.nodes], [])])})

    def _preprocess_threads(self, ex_job):
        """Prepare execution description for threads execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = ex_job.job_execution.exec
        job_args = ex_job.job_execution.args

        core_ids = []
        if self.resources.binding:
            for node in ex_job.allocation.nodes:
                for slot in node.cores:
                    core_ids.extend(slot.split(','))

        cpu_mask = 0
        if self.resources.binding:
            for cpu in core_ids:
                cpu_mask = cpu_mask | 1 << int(cpu)
            cpu_bind = "--cpu-bind=verbose,mask_cpu:{}".format(hex(cpu_mask))
        else:
            cpu_bind = "--cpu-bind=verbose,cores"

        ex_job.job_execution.args = [
            "-n", "1",
            "--cpus-per-task", str(ex_job.ncores),
            "--overcommit",
            "--mem-per-cpu=0",
            cpu_bind]

        self._preprocess_common(ex_job)

        ex_job.job_execution.exec = 'srun'
        ex_job.job_execution.args.extend([job_exec, *job_args])

    def _preprocess_default(self, ex_job):
        """Prepare execution description for default execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = ex_job.job_execution.exec
        job_args = ex_job.job_execution.args

        run_conf_file = os.path.join(ex_job.wd_path, ".{}.runconfig".format(ex_job.job_iteration.name))
        with open(run_conf_file, 'w') as conf_f:
            conf_f.write("0\t%s %s\n" % (
                job_exec,
                ' '.join('{0}'.format(str(arg).replace(" ", "\\ ")) for arg in job_args)))
            if ex_job.ncores > 1:
                if ex_job.ncores > 2:
                    conf_f.write("1-%d /bin/true\n" % (ex_job.ncores - 1))
                else:
                    conf_f.write("1 /bin/true\n")

        if self.resources.binding:
            core_ids = []
            for node in ex_job.allocation.nodes:
                core_ids.extend([str(core) for core in node.cores])
            cpu_bind = "--cpu-bind=verbose,map_cpu:{}".format(','.join(core_ids))
        else:
            cpu_bind = "--cpu-bind=verbose,cores"

        ex_job.job_execution.args = [
            "-n", str(ex_job.ncores),
            "--overcommit",
            "--mem-per-cpu=0",
            cpu_bind,
            "--multi-prog"]

        self._preprocess_common(ex_job)

        ex_job.job_execution.exec = 'srun'
        ex_job.job_execution.args.append(run_conf_file)

    def _preprocess_openmpi(self, ex_job):
        """Prepare execution description for openmpi execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = ex_job.job_execution.exec
        job_args = ex_job.job_execution.args

        # create rank file
        if self.resources.binding:
            rank_file = os.path.join(ex_job.wd_path, ".{}.rankfile".format(ex_job.job_iteration.name))
            rank_id = 0
            with open(rank_file, 'w') as rank_f:
                for node in ex_job.allocation.nodes:
                    for core in node.cores:
                        rank_f.write(f'rank {rank_id}={node.node.name} slot={core}\n')
                        rank_id = rank_id + 1

            mpi_args = [
                '--mca rmaps_rank_file_physical 1',
                '--rankfile',
                str(rank_file),
            ]
        else:
            mpi_args = [
                '-n',
                str(ex_job.ncores),
            ]

        ex_job.job_execution.exec = 'bash'
        ex_job.job_execution.args = ['-c',
                'source /etc/profile; module purge; module load {}; exec mpirun {} {} {}'.format(
                    Config.OPENMPI_MODEL_MODULE.get(self.config), ' '.join(mpi_args), job_exec, '' if not job_args else ' '.join(job_args))]
#        ex_job.job_execution.args.extend([job_exec, *job_args])

    def _preprocess_intelmpi(self, ex_job):
        """Prepare execution description for intelmpi execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = ex_job.job_execution.exec
        job_args = ex_job.job_execution.args

        mpi_args = []
        first = True

        # create rank file
        if self.resources.binding:

            for node in ex_job.allocation.nodes:
                if not first:
                    mpi_args.append(':')

                mpi_args.extend([
                    '-host',
                    f'{node.node.name}',
                    '-n',
                    f'{len(node.cores)}',
                    '-env',
                    f'I_MPI_PIN_PROCESSOR_LIST={",".join([str(core) for core in node.cores])}',
                    f'{job_exec}',
                    *job_args])

                first = False

            ex_job.env.update({'I_MPI_PIN': '1'})
        else:
            mpi_args = ['-n', f'{str(ex_job.ncores)}', f'{job_exec}']

        if top_logger.level == logging.DEBUG:
            ex_job.env.update({'I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS':
                                   '-vvvvvv --overcommit --oversubscribe --cpu-bind=none --mem-per-cpu=0'})
            ex_job.env.update({'I_MPI_HYDRA_DEBUG': '1'})
            ex_job.env.update({'I_MPI_DEBUG': '5'})
        else:
            ex_job.env.update({'I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS':
                                   '-v --overcommit --oversubscribe --mem-per-cpu=0'})

        ex_job.job_execution.exec = 'mpirun'
        ex_job.job_execution.args = [*mpi_args]
        if job_args:
            ex_job.job_execution.args.extend(job_args)

    def _preprocess_srunmpi(self, ex_job):
        """Prepare execution description for mpi with slurm's srun execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = ex_job.job_execution.exec
        job_args = ex_job.job_execution.args

        mpi_args = []
        first = True

        # create rank file
        if self.resources.binding:
            cpu_masks = []
            for node in ex_job.allocation.nodes:
                for slot in node.cores:
                    cpu_mask = 0
                    for cpu in slot.split(','):
                        cpu_mask = cpu_mask | 1 << int(cpu)
                    cpu_masks.append(hex(cpu_mask))
            cpu_bind = "--cpu-bind=verbose,mask_cpu:{}".format(','.join(cpu_masks))
        else:
            cpu_bind = "--cpu-bind=verbose,cores"

        ex_job.job_execution.exec = 'srun'
        ex_job.job_execution.args = [
            "-n", str(ex_job.ncores),
            "--overcommit",
            "--mem-per-cpu=0",
            "-m", "arbitrary",
            cpu_bind ]

        ex_job.job_execution.args.append(job_exec)
        if job_args:
            ex_job.job_execution.args.extend(job_args)

    def preprocess(self, ex_job):
        """"Preprocess job iteration description before launching.
        Prepare job iteration execution arguments.

        Args
            ex_job (ExecutionJob): execution job iteration data
        """
        # as the single core jobs are launched directly by the agent or locally without slurm interaction
        # this preprocess should be executed only for parallel jobs
        if len(ex_job.allocation.nodes) != 1 or ex_job.allocation.nodes[0].ncores != 1:
            job_model = ex_job.job_execution.model or 'default'

            _logger.debug(f'looking for job model {job_model}')

            preprocess_method = SlurmExecution.JOB_MODELS.get(job_model)
            if not preprocess_method:
                raise InternalError(f"unknown job execution model '{job_model}'")

            method = getattr(self, preprocess_method)
            method(ex_job)


class DirectExecution(ExecutionSchema):
    """Directly execute job iteration without any proxy commands."""

    EXEC_NAME = 'direct'

    def preprocess(self, ex_job):
        """"Preprocess job iteration description before launching.
        Prepare job iteration execution arguments.

        Args
            ex_job (ExecutionJob): execution job iteration data
        """

    def get_env_opts(self):
        """Return options for environment instances.

        Set environments to not create 'hostfile'
        """
        return {'nohostfile': True}


__SCHEMAS__ = {
    ResourcesType.SLURM: SlurmExecution,
    ResourcesType.LOCAL: DirectExecution
}
