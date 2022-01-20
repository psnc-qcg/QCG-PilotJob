import logging
import os
import subprocess
import sys

from math import log2, floor

from qcg.pilotjob.common.errors import SlurmEnvError
from qcg.pilotjob.common.resources import CRType, CRBind, Node, Resources, ResourcesType
from qcg.pilotjob.common.config import Var


def parse_local_cpus():
    """Return information about available CPU's and cores in local system.
    The information is gathered from ``lscpu`` command which besides the available CPUs informations, also returns
    information about physical cores in the system, which is usefull for hyper threading systems.

    Returns:
        dict(str, list), dict(str, list): two maps with mapping:
            core_id -> list of cpu's assigned to core
            cpu_id -> list of cores (in most situations this will be a single element list)
    """
    cores = {}
    cpus = {}

    proc = subprocess.Popen(['lscpu', '-p'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    ex_code = proc.wait()
    if ex_code != 0:
        raise Exception("command lscpu failed with exit code {}: {}".format(ex_code, stderr))

    for out_line in bytes.decode(stdout).splitlines():
        # omit all commented lines
        if out_line.startswith('#'):
            continue

        elems = out_line.split(',')
        if len(elems) != 9:
            logging.warning('warning: unknown output format "{}"'.format(out_line))

        cpu,core,socket = elems[0:3]

        cores.setdefault(core, []).append(cpu)
        cpus.setdefault(cpu, []).append(core)

    return cores, cpus


def parse_nodelist(nodespec):
    """Return full node names based on the Slurm node specification.

    This method calls ``scontrol show hostnames`` to get real node host names.

    Args:
        nodespec (str): Slurm node specification

    Returns:
        list(str): node hostnames
    """
    proc = subprocess.Popen(['scontrol', 'show', 'hostnames', nodespec], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    ex_code = proc.wait()
    if ex_code != 0:
        raise SlurmEnvError("scontrol command failed: {}".format(stderr))

    return bytes.decode(stdout).splitlines()


def get_allocation_data():
    """Get information about slurm allocation and pack it into dictionary.
    The information is obtained by 'scontrol show job' command

    Returns:
        dict(str,str): list of all allocation attributes and values and also a dictionary, a dictionary
            might be used to check if any element exist in attributes, but for some attributes like Node, CPU_IDs they
            are not uniq so in the map there will be just the last occurence of these attributes; remember that
            the dictionary doesn't contain information about attributes order.
    """
    slurm_job_id = os.environ.get('SLURM_JOB_ID', None)
    proc = subprocess.Popen(['scontrol', 'show', '-o', '--detail', 'job', slurm_job_id], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    ex_code = proc.wait()
    if ex_code != 0:
        raise SlurmEnvError("scontrol show job failed: {}".format(stderr))

    raw_data = bytes.decode(stdout).replace('\n', ' ')
    result = {'list': [], 'map': {}}
    for element in raw_data.split(' '):
        elements = element.split('=', 1)
        name = elements[0]
        value = elements[1] if len(elements) > 1 else None

        result['map'][name] = value
        result['list'].append((name, value))

    return result


def parse_slurm_cpu_binding(cpu_bind_list):
    """Return CPU identifier list based on Slurm's SLURM_CPU_BIND_LIST variable's value.

    Args:
        cpu_bind_list (str): the value of SLURM_CPU_BIND_LIST

    Returns:
        list (int): list of CPU identifiers
    """
    cores = []
    for hex_mask in cpu_bind_list.split(','):
        mask = int(hex_mask, 0)

        cores.extend([i for i in range(int(log2(mask)) + 1) if mask & (1 << i)])

    # sort uniq values
    return sorted(list(set(cores)))


def parse_slurm_job_cpus(cpus):
    """Return number of cores allocated on each node in the allocation.

    This method parses value of Slurm's SLURM_JOB_CPUS_PER_NODE variable's value.

    Args:
        cpus (str): the value of SLURM_JOB_CPUS_PER_NODE

    Returns:
        list (int): the number of cores on each node
    """
    result = []

    for part in cpus.split(','):
        if part.find('(') != -1:
            cores, times = part.rstrip(')').replace('(x', 'x').split('x')
            for _ in range(0, int(times)):
                result.append(int(cores))
        else:
            result.append(int(part))

    return result


def get_num_slurm_nodes():
    """Return number of nodes in Slurm allocation.

    Returns:
        int: number of nodes
    """
    if 'SLURM_NODELIST' not in os.environ:
        raise SlurmEnvError("missing SLURM_NODELIST settings")

    return len(parse_nodelist(os.environ['SLURM_NODELIST']))


def parse_slurm_resources(config):
    """Return resources availabe in Slurm allocation.

    Args:
        config (dict): QCG-PilotJob configuration

    Returns:
        Resources: resources available in Slurm allocation
    """
    if 'SLURM_NODELIST' not in os.environ:
        raise SlurmEnvError("missing SLURM_NODELIST settings")

    if 'SLURM_JOB_CPUS_PER_NODE' not in os.environ:
        raise SlurmEnvError("missing SLURM_JOB_CPUS_PER_NODE settings")

    allocation_data = None
    allocation_data = get_allocation_data()

    slurm_nodes = os.environ['SLURM_NODELIST']

    node_names = parse_nodelist(slurm_nodes)

    node_start = config.get(Var.SLURM_LIMIT_NODES_RANGE_BEGIN)
    if not node_start:
        node_start = 0

    node_end = config.get(Var.SLURM_LIMIT_NODES_RANGE_END)
    if not node_end:
        node_end = len(node_names)
    else:
        node_end = min(node_end, len(node_names))

    logging.debug('node range %d - %d from %d total nodes', node_start, node_end, len(node_names))

    if node_start > node_end:
        raise SlurmEnvError(
            'Invalid node range arguments - node start {} greater than node end {}'.format(node_start, node_end))

    slurm_job_cpus_def = os.environ['SLURM_JOB_CPUS_PER_NODE']
    job_cpus = parse_slurm_job_cpus(slurm_job_cpus_def)

    slurm_tasks_def = os.environ['SLURM_TASKS_PER_NODE']
    job_tasks = parse_slurm_job_cpus(slurm_tasks_def)


    if config.get(Var.PARENT_MANAGER) and len(node_names) != len(job_tasks):
        # TODO: dirty hack - to fix problems with ht & governor manaagers
        job_tasks = job_cpus

    if len(node_names) != len(job_cpus) or len(node_names) != len(job_tasks):
        raise SlurmEnvError("failed to parse slurm env: number of nodes ({}) mismatch number of job cpus ({})"
                            "/job tasks ({}), ({})".format(len(node_names), len(job_cpus), len(job_tasks), slurm_tasks_def))
    core_ids = None
    binding = False

#    slots_num = job_cpus
    slots_num = job_tasks

    if allocation_data is not None and 'CPU_IDs' in allocation_data['map']:
        cpu_ids = parse_slurm_allocation_cpu_ids(allocation_data['list'], node_names[node_start:node_end],
                                                 job_cpus[node_start:node_end])
        logging.debug('got available cpus per node: {}'.format(','.join(
            ['{}: [{}]'.format(node_name, ','.join([str(core) for core in node_cores]))
             for node_name, node_cores in cpu_ids.items()])))

        core_ids = {node_name: [str(cpu_id) for cpu_id in node_cpus] for node_name, node_cpus in cpu_ids.items()}
        logging.debug('temporary core ids per node: {}'.format(','.join(
            ['{}: [{}]'.format(node_name, ','.join([str(core) for core in node_cores]))
             for node_name, node_cores in cpu_ids.items()])))


        if job_cpus != job_tasks and all(job_cpus[i] == job_cpus[i+1] for i in range(len(job_cpus) - 1)):
            # the number of available cpu's differs from number of requested tasks - two situations:
            #  a) user want's execute less tasks than available cpu's
            #  b) hyper-threading (many cpus for single core)
            # in both situations we should execute only as much jobs as requested tasks
            # with hyper-threading we should also bind many cpus to a single job
            # to be able to assign system cpu's to physical cores we need information about cpu - for example from
            # execution of 'lscpu' - which need to be execute on each node - but currently we assume that all nodes
            # in allocation are homogenous
            logging.debug('number of tasks ({}) differs from number of cpus ({}) - checking local cpus vs cores'.format(','.join([str(task) for task in job_tasks]), ','.join([str(cpu) for cpu in job_cpus])))
            local_core_info, local_cpu_info = parse_local_cpus()

#            if len(local_core_info) != len(local_cpu_info):

            logging.info("number of available cpu's {} differs from number of available cores {} - "
                         "HyperThreading".format(len(local_core_info), len(local_cpu_info)))

            core_ids = {}
            # group available cpu's into cores
            for node_idx in range(node_start, node_end):
                node_name = node_names[node_idx]
                node_cpus = cpu_ids[node_name]
                node_tasks = job_tasks[node_idx]

                if len(local_core_info) ==  node_tasks:
                    # number of tasks on a node is the same as number of cores
                    core_ids[node_name] = [','.join(local_core_info[core_id]) for core_id in local_core_info]
                else:
                    # partition available cpu's on given number of tasks
                    if node_tasks < len(local_core_info):
                        # partition available cores on given number of tasks
                        avail_cores = len(local_core_info)
                        first_core = 0

                        node_task_cpu_ids = []
                        for task_nr in range(node_tasks):
                            node_cores = floor(avail_cores / (node_tasks - task_nr))

#                            logging.debug('assigning #{} ({} - {}) cores for task {}'.format(node_cores, first_core, first_core + node_cores, task_nr))
                            cores_cpu_list = []
                            for core_id in range(first_core, first_core + node_cores):
                                cores_cpu_list.extend(local_core_info[str(core_id)])
                            node_task_cpu_ids.append(','.join(cores_cpu_list))
#                            logging.debug('assinged ({}) cpus for task {}'.format(node_task_cpu_ids[-1], task_nr))

                            first_core += node_cores
                            avail_cores -= node_cores

                        core_ids[node_name] = node_task_cpu_ids
                    else:
                        # TODO: this needs futher work
                        raise Exception('Not supported')

        binding = True
    elif 'SLURM_CPU_BIND_LIST' in os.environ and 'SLURM_CPU_BIND_TYPE' in os.environ and \
            os.environ['SLURM_CPU_BIND_TYPE'].startswith('mask_cpu'):
        core_ids = parse_slurm_env_binding(os.environ['SLURM_CPU_BIND_LIST'], node_names[node_start:node_end],
                                           job_cpus[node_start:node_end])
        binding = True
    else:
        logging.warning('warning: failed to get slurm binding information - missing SLURM_CPU_BIND_LIST and CPU_IDSs')

    if binding:
        logging.debug('core binding for {} nodes: {}'.format(len(core_ids),
            '; '.join(['{}: {}'.format(str(node), str(slots)) for node,slots in core_ids.items()])))
    else:
        logging.warning('warning: no binding information')

    n_crs = None
    if 'CUDA_VISIBLE_DEVICES' in os.environ:
        n_crs = {CRType.GPU: CRBind(CRType.GPU, os.environ['CUDA_VISIBLE_DEVICES'].split(','))}

    nodes = [Node(node_names[i], slots_num[i], 0, core_ids=core_ids[node_names[i]] if core_ids is not None else None,
                  crs=n_crs) for i in range(node_start, node_end)]
    logging.debug('generated %d nodes %s binding', len(nodes), 'with' if binding else 'without')

    return Resources(ResourcesType.SLURM, nodes, binding)


def parse_slurm_allocation_cpu_ids(allocation_data_list, node_names, cores_num):
    """Based on allocation data obtained via 'scontrol show job --detail' return information about core bindings per
    node.
    The information in the allocation data is optimized, so getting those binding might be tricky.
    The data can be described in form:

        Nodes=c[1-2] CPU_IDs=0 Mem=0 GRES=

    but also as:

        Nodes=c1 CPU_IDs=1 Mem=0 GRES=
        Nodes=c2 CPU_IDs=0 Mem=0 GRES=

    Args:
        allocation_data_list (dict): allocation data (as map ('map' key) and list ('list'))
        node_names (list(str)): node names for which the binding should be parsed
        cores_num (list(int)): the number of allocated cores for given nodes (the binding information must match #
            of cores)

    Returns:
        dict: map with node names as keys and binded core list as value
    """
    nodes = {}

    try:
        curr_nodes = []
        for param in allocation_data_list:
            name, value = param

            if name == 'Nodes':
                curr_node = value
                if any(c in value for c in ['-', ',', '[', ']']):
                    curr_nodes.extend(parse_nodelist(value))
                else:
                    curr_nodes.append(value)
            elif name == 'CPU_IDs':
                core_ids = []
                for element in value.split(','):
                    if '-' in element:
                        start, end = element.split('-', 1)
                        core_ids.extend(list(range(int(start), int(end) + 1)))
                    else:
                        core_ids.append(int(element))

                if curr_nodes is None:
                    raise Exception('missing node name in allocation data')

                for curr_node in curr_nodes:
                    if curr_node in node_names:
                        nodes[curr_node] = core_ids

                curr_nodes = []
    except Exception as exc:
        raise SlurmEnvError('unknown format of cpu binding: {}'.format(str(exc)))

    if len(nodes) != len(node_names):
        raise SlurmEnvError('failed to parse cpu binding from job info: the node binding list ({}) differes from '
                            'node list ({}): {}'.format(len(nodes), len(node_names), str(allocation_data_list)))

    if len(nodes) != len(cores_num):
        raise SlurmEnvError('failed to parse cpu binding from job info: the node binding list ({}) differes from '
                            'ncores per node list ({})'.format(len(nodes), len(cores_num)))

    for idx, node in enumerate(nodes.keys()):
        if len(nodes[node]) != cores_num[idx]:
            raise SlurmEnvError('failed to parse cpu binding: the node binding for node ({}) ({}) differs from cores '
                                'per node {}'.format(node, len(nodes[node]), cores_num[idx]))

    return nodes


def parse_slurm_env_binding(slurm_cpu_bind_list, node_names, cores_num):
    """Based on environment varialbe SLURM_CPU_BIND_LIST set by slurm return information about core bindings per node.
    WARNING: those information might not be as precise as those obtained from allocation data, as environment variable
    contain the same information for all nodes, so if not all nodes has the same architecture and number of allocated
    cores the information might not be correct.

    Args:
        slurm_cpu_bind_list (str): value of SLURM_CPU_BIND_LIST variable
        node_names (list(str)): name of the nodes
        cores_num (list(int)): number of cores on each node

    Returns:
        dict(str,int): map with node names and core identifier list
    """
    core_ids = parse_slurm_cpu_binding(slurm_cpu_bind_list)

    if len(core_ids) < max(cores_num):
        raise SlurmEnvError("failed to parse cpu binding: the core list ({}) mismatch the cores per node ({})".format(
            str(core_ids), str(cores_num)))

    logging.debug("cpu list on each node: %s", str(core_ids))

    return {node_name: core_ids for node_name in node_names}


def in_slurm_allocation():
    """Check if program has been run inside slurm allocation.
    We detect some environment variables (like SLURM_NODELIST) that are always set by slurm.

    :return: true if we are inside slurm allocation, otherwise false
    """
    return 'SLURM_NODELIST' in os.environ and 'SLURM_JOB_CPUS_PER_NODE' in os.environ


def test_environment(env=None):
    """Try to parse slurm resources based on environment passed as dictionary, or string where each
    environment variable is placed in the separate line.
    WARNING: some information must be gathered through slurm client programs like 'scontrol' so gathering resource
    information only on environment variables is limited.

    Args:
        env (dict|string,optional): environment to test, if
            None - the current environment is checked
            dict - the environment in form of dictionary to be checked
            string - the environment in form of string where each variable is placed in separate line

    Returns:
        Resources: instance with gathered slurm information.
    """
    if not env:
        env_d = os.environ
    elif isinstance(env, (dict, os._Environ)):
        env_d = env
    elif isinstance(env, str):
        env_d = {line.split('=', 1)[0]: line.split('=', 1)[1]
                 for line in env.splitlines() if len(line.split('=', 1)) == 2}
    else:
        raise ValueError('Wrong type of argument ({}) - only string/dict are allowed'.format(type(env).__name__))

    old_os_environ = os.environ
    try:
        os.environ = env_d
        result = parse_slurm_resources({'parse_nodes': 'no'})
    finally:
        os.environ = old_os_environ

    print('parsed SLURM resources: {}'.format(result))
    return result
