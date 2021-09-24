import os
import logging
import types

from qcg.pilotjob.slurmres import in_slurm_allocation
from qcg.pilotjob.resources import CRType


_logger = logging.getLogger(__name__)


class Environment:
    """Base class for setting job's environment variables.

    Attributes:
        NAME (str): name of environment class

    All parent classes must implement ``update_env`` method.
    """
    NAME = 'abstract'

    def __init__(self):
        pass

    def update_env(self, job, env, opts=None):
        """Update job environment.

        Args:
            job (ExecutionJob): job data
            env (dict(str,str)): environment to update
            opts (dict(str,str), optional): optional preferences for generating environment
        """
        raise NotImplementedError()


class CommonEnvironment(Environment):
    """The common environment for all execution schemas."""

    NAME = 'common'

    def __init__(self):
        super(CommonEnvironment, self).__init__()
        _logger.info('initializing COMMON environment')

    def update_env(self, job, env, opts=None):
        _logger.debug('updating common environment')

        env.update({
            'QCG_PM_NNODES': str(job.nnodes),
            'QCG_PM_NODELIST': job.nlist,
            'QCG_PM_NPROCS': str(job.ncores),
            'QCG_PM_NTASKS': str(job.ncores),
            'QCG_PM_STEP_ID': str(job.jid),
            'QCG_PM_TASKS_PER_NODE': job.tasks_per_node
        })


class SlurmEnvironment(Environment):
    """The environment compatible with Slurm execution environments."""

    NAME = 'slurm'

    def __init__(self):
        super(SlurmEnvironment, self).__init__()
        _logger.info('initializing SLURM environment')

    @staticmethod
    def _merge_per_node_spec(str_list):
        prev_value = None
        result = []
        times = 1

        for elem in str_list.split(','):
            if prev_value is not None:
                if prev_value == elem:
                    times += 1
                else:
                    if times > 1:
                        result.append("%s(x%d)" % (prev_value, times))
                    else:
                        result.append(prev_value)

                    prev_value = elem
                    times = 1
            else:
                prev_value = elem
                times = 1

        if prev_value is not None:
            if times > 1:
                result.append("%s(x%d)" % (prev_value, times))
            else:
                result.append(prev_value)

        return ','.join([str(el) for el in result])

    @staticmethod
    def _check_same_cores(tasks_list):
        same = None

        for elem in tasks_list.split(','):
            if same is not None:
                if elem != same:
                    return None
            else:
                same = elem

        return same

    def update_env(self, job, env, opts=None):
        merged_tasks_per_node = SlurmEnvironment._merge_per_node_spec(job.tasks_per_node)

        job.env.update({
            'SLURM_NNODES': str(job.nnodes),
            'SLURM_NODELIST': job.nlist,
            'SLURM_NPROCS': str(job.ncores),
            'SLURM_NTASKS': str(job.ncores),
            'SLURM_JOB_NODELIST': job.nlist,
            'SLURM_JOB_NUM_NODES': str(job.nnodes),
            'SLURM_STEP_NODELIST': job.nlist,
            'SLURM_STEP_NUM_NODES': str(job.nnodes),
            'SLURM_STEP_NUM_TASKS': str(job.ncores),
            'SLURM_JOB_CPUS_PER_NODE': merged_tasks_per_node,
            'SLURM_STEP_TASKS_PER_NODE': merged_tasks_per_node,
            'SLURM_TASKS_PER_NODE': merged_tasks_per_node
        })

        same_cores = SlurmEnvironment._check_same_cores(job.tasks_per_node)
        if same_cores is not None:
            job.env.update({'SLURM_NTASKS_PER_NODE': same_cores})

        if not opts or not opts.get('nohostfile', False):
            # create host file
            hostfile = os.path.join(job.wd_path, ".{}.hostfile".format(job.job_iteration.name))
            with open(hostfile, 'w') as hostfile_h:
                for node in job.allocation.nodes:
                    for _ in range(0, node.ncores):
                        hostfile_h.write("{}\n".format(node.node.name))
            job.env.update({
                'SLURM_HOSTFILE': hostfile
            })
            _logger.debug('host file generated at %s', hostfile)
        else:
            _logger.debug('not generating hostfile')

        node_with_gpu_crs = [node for node in job.allocation.nodes
                             if node.crs is not None and CRType.GPU in node.crs]
        if node_with_gpu_crs:
            # as currenlty we have no way to specify allocated GPU's per node, we assume that all
            # nodes has the same settings
            job.env.update({'CUDA_VISIBLE_DEVICES': ','.join(node_with_gpu_crs[0].crs[CRType.GPU].instances)})
        else:
            # remote CUDA_VISIBLE_DEVICES for allocations without GPU's
            if 'CUDA_VISIBLE_DEVICES' in job.env:
                del job.env['CUDA_VISIBLE_DEVICES']


def _select_auto_environment():
    """Select proper job execution environment.

    When QCG-PilotJob manager is executed inside Slurm allocation, the same execution environment is returned.
    For local modes, the base (common) environment is used.

    Returns:
        Environment: the selected job execution environment
    """
    if in_slurm_allocation():
        return SlurmEnvironment

    return CommonEnvironment


_available_envs = {
    # List of all available environments.

    'auto': _select_auto_environment,
    CommonEnvironment.NAME: CommonEnvironment,
    SlurmEnvironment.NAME: SlurmEnvironment
}


def get_environment(env_name):
    """Return job execution environment based on the name.

    Args:
        env_name (str): environment name

    Returns:
        Environment: the environment with selected name

    Raises:
        ValueError: if environment with given name is not available
    """
    if env_name not in _available_envs:
        raise ValueError('environment "{}" not available'.format(env_name))

    env_type = _available_envs[env_name]
    return env_type() if isinstance(env_type, types.FunctionType) else env_type
