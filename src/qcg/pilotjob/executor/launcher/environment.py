import os
import logging

from qcg.pilotjob.common.resources import CRType


class CommonEnvironment:
    """The common environment for all execution schemas."""

    @staticmethod
    def update_env(executing_iteration, env, opts=None):
        logging.debug('updating common environment')

        allocation = executing_iteration.allocation
        jid = executing_iteration.id

        tasks_per_node = ','.join([str(len(node.get('cores'))) for node in allocation.nodes])
        nlist = ','.join([node.get('name') for node in allocation.nodes])

        env.update({
            'QCG_PM_NNODES': str(len(allocation.nodes)),
            'QCG_PM_NODELIST': nlist,
            'QCG_PM_NPROCS': str(allocation.cores),
            'QCG_PM_NTASKS': str(allocation.cores),
            'QCG_PM_STEP_ID': str(jid),
            'QCG_PM_TASKS_PER_NODE': tasks_per_node
        })


class SlurmEnvironment:
    """The environment compatible with Slurm execution environments."""

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

    @staticmethod
    def update_env(executing_iteration, env, opts=None):
        allocation = executing_iteration.allocation
        jid = executing_iteration.id

        tasks_per_node = ','.join([str(len(node["cores"])) for node in allocation.nodes])
        nnodes = len(allocation.nodes)
        nlist = ','.join([node["name"] for node in allocation.nodes])

        merged_tasks_per_node = SlurmEnvironment._merge_per_node_spec(tasks_per_node)

        env.update({
            'SLURM_NNODES': str(nnodes),
            'SLURM_NODELIST': nlist,
            'SLURM_NPROCS': str(allocation.cores),
            'SLURM_NTASKS': str(allocation.cores),
            'SLURM_JOB_NODELIST': nlist,
            'SLURM_JOB_NUM_NODES': str(nnodes),
            'SLURM_STEP_NODELIST': nlist,
            'SLURM_STEP_NUM_NODES': str(nnodes),
            'SLURM_STEP_NUM_TASKS': str(allocation.cores),
            'SLURM_JOB_CPUS_PER_NODE': merged_tasks_per_node,
            'SLURM_STEP_TASKS_PER_NODE': merged_tasks_per_node,
            'SLURM_TASKS_PER_NODE': merged_tasks_per_node
        })

        same_cores = SlurmEnvironment._check_same_cores(tasks_per_node)
        if same_cores is not None:
            env.update({'SLURM_NTASKS_PER_NODE': same_cores})

        node_with_gpu_crs = [node for node in allocation.nodes
                             if node.get("crs") is not None and CRType.GPU in node.get("crs")]
        if node_with_gpu_crs:
            # as currenlty we have no way to specify allocated GPU's per node, we assume that all
            # nodes has the same settings
            env.update({'CUDA_VISIBLE_DEVICES': ','.join(node_with_gpu_crs[0]["crs"][CRType.GPU].instances)})
        else:
            # remote CUDA_VISIBLE_DEVICES for allocations without GPU's
            if 'CUDA_VISIBLE_DEVICES' in env:
                del env['CUDA_VISIBLE_DEVICES']
