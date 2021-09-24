import pytest
from os import environ, mkdir, stat
from os.path import dirname, join, isdir, exists, abspath
import tempfile
import logging
import asyncio
import time
from shutil import rmtree
import sys

from qcg.pilotjob.service import QCGPMService
from qcg.pilotjob.slurmres import in_slurm_allocation, get_num_slurm_nodes
from qcg.pilotjob.executionjob import LauncherExecutionJob
from qcg.pilotjob.joblist import Job, JobExecution, JobResources, ResourceSize, JobDependencies
from qcg.pilotjob.tests.utils import save_reqs_to_file, check_job_status_in_json, get_slurm_resources, \
    get_slurm_resources_binded, set_pythonpath_to_qcg_module

from qcg.pilotjob.tests.utils import SHARED_PATH


def test_slurmenv_simple_resources():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    get_slurm_resources()


def test_slurmenv_simple_resources_binding():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    print('SLURM_NODELIST: {}'.format(environ.get('SLURM_NODELIST', None)))
    print('SLURM_JOB_CPUS_PER_NODE: {}'.format(environ.get('SLURM_JOB_CPUS_PER_NODE', None)))
    print('SLURM_CPU_BIND_LIST: {}'.format(environ.get('SLURM_CPU_BIND_LIST', None)))
    print('SLURM_CPU_BIND_TYPE: {}'.format(environ.get('SLURM_CPU_BIND_TYPE', None)))
    print('CUDA_VISIBLE_DEVICES: {}'.format(environ.get('CUDA_VISIBLE_DEVICES', None)))

    get_slurm_resources_binded()



#def test_slurmenv_simple_job(caplog):
#   caplog.set_level(logging.DEBUG)
def test_slurmenv_simple_job():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'mdate'
    jobs = [job.to_dict() for job in [
        Job(jobName,
            JobExecution(
                'date',
                wd = abspath(join(tmpdir, 'date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    jobEntries = check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='SUCCEED')
    assert all((isdir(abspath(join(tmpdir, 'date.sandbox'))),
                exists(join(abspath(join(tmpdir, 'date.sandbox')), 'date.out')),
                exists(join(abspath(join(tmpdir, 'date.sandbox')), 'date.err')),
                stat(join(abspath(join(tmpdir, 'date.sandbox')), 'date.out')).st_size > 0))
    # there can be some debugging messages in the stderr
#                stat(join(abspath(join(tmpdir, 'date.sandbox')), 'date.err')).st_size == 0))

    for jname, jentry in jobEntries.items():
        assert all(('runtime' in jentry, 'allocation' in jentry.get('runtime', {})))

        jalloc = jentry['runtime']['allocation']
        for jalloc_node in jalloc.split(','):
            node_name = jalloc_node[:jalloc_node.index('[')]
            print('{} in available nodes ({})'.format(node_name, ','.join(resources_node_names)))
            assert node_name in resources_node_names, '{} not in nodes ({}'.format(
                node_name, ','.join(resources_node_names))

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=tmpdir, dest_state='SUCCEED')

    rmtree(tmpdir)


def test_slurmenv_simple_script():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'mdate_script'
    jobs = [job.to_dict() for job in [
        Job(jobName,
            JobExecution(
                script = '/bin/date\n/bin/hostname\n',
                wd = abspath(join(tmpdir, 'date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    jobEntries = check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='SUCCEED')
    assert all((isdir(abspath(join(tmpdir, 'date.sandbox'))),
                exists(join(abspath(join(tmpdir, 'date.sandbox')), 'date.out')),
                exists(join(abspath(join(tmpdir, 'date.sandbox')), 'date.err')),
                stat(join(abspath(join(tmpdir, 'date.sandbox')), 'date.out')).st_size > 0,
                stat(join(abspath(join(tmpdir, 'date.sandbox')), 'date.err')).st_size == 0))

    for jname, jentry in jobEntries.items():
        assert all(('runtime' in jentry, 'allocation' in jentry.get('runtime', {})))

        jalloc = jentry['runtime']['allocation']
        for jalloc_node in jalloc.split(','):
            node_name = jalloc_node[:jalloc_node.index('[')]
            print('{} in available nodes ({})'.format(node_name, ','.join(resources_node_names)))
            assert node_name in resources_node_names, '{} not in nodes ({}'.format(
                node_name, ','.join(resources_node_names))

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=tmpdir, dest_state='SUCCEED')

    rmtree(tmpdir)


def test_slurmenv_many_cores():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'hostname'
    jobwdir_base = 'hostname.sandbox'
    cores_num = 2
    jobs = [job.to_dict() for job in [
        Job(jobName,
            JobExecution(
                exec = 'mpirun',
                args = [ '--allow-run-as-root', 'hostname' ],
                wd = abspath(join(tmpdir, jobwdir_base)),
                stdout = 'hostname.out',
                stderr = 'hostname.err',
                modules = [ 'mpi/openmpi-x86_64' ]
            ),
            JobResources(numCores=ResourceSize(cores_num) )
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    jobEntries = check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='SUCCEED')
    assert all((isdir(abspath(join(tmpdir, jobwdir_base))),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.err')),
                stat(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')).st_size > 0))

    job_nodes = []
    allocated_cores = 0
    for jname, jentry in jobEntries.items():
        assert all(('runtime' in jentry, 'allocation' in jentry.get('runtime', {})))

        jalloc = jentry['runtime']['allocation']
        for jalloc_node in jalloc.split(','):
            node_name = jalloc_node[:jalloc_node.index('[')]
            job_nodes.append(node_name)
            print('{} in available nodes ({})'.format(node_name, ','.join(resources_node_names)))
            assert node_name in resources_node_names, '{} not in nodes ({}'.format(
                node_name, ','.join(resources_node_names))

            ncores = len(jalloc_node[jalloc_node.index('[') + 1:-1].split(':'))
            allocated_cores += ncores
    assert allocated_cores == cores_num, allocated_cores

    # check if hostname is in stdout in two lines
    with open(abspath(join(tmpdir, join(jobwdir_base, 'hostname.out'))), 'rt') as stdout_file:
        stdout_content = [line.rstrip() for line in stdout_file.readlines()]
    assert len(stdout_content) == cores_num, str(stdout_content)
    assert all(hostname in job_nodes for hostname in stdout_content), str(stdout_content)

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=tmpdir, dest_state='SUCCEED')

    rmtree(tmpdir)

def test_slurmenv_many_nodes():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'hostname'
    jobwdir_base = 'hostname.sandbox'
    cores_num = 1
    nodes_num = 2
    jobs = [job.to_dict() for job in [
        Job(jobName,
            JobExecution(
                exec = 'mpirun',
                args = [ '--allow-run-as-root', 'hostname' ],
                wd = abspath(join(tmpdir, jobwdir_base)),
                stdout = 'hostname.out',
                stderr = 'hostname.err',
                modules = [ 'mpi/openmpi-x86_64' ]
            ),
            JobResources(numCores=ResourceSize(cores_num), numNodes=ResourceSize(nodes_num))
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    jobEntries = check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='SUCCEED')
    assert all((isdir(abspath(join(tmpdir, jobwdir_base))),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.err')),
                stat(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')).st_size > 0))

    job_nodes = []
    allocated_cores = 0
    for jname, jentry in jobEntries.items():
        assert all(('runtime' in jentry, 'allocation' in jentry.get('runtime', {})))

        jalloc = jentry['runtime']['allocation']
        for jalloc_node in jalloc.split(','):
            node_name = jalloc_node[:jalloc_node.index('[')]
            job_nodes.append(node_name)
            print('{} in available nodes ({})'.format(node_name, ','.join(resources_node_names)))
            assert node_name in resources_node_names, '{} not in nodes ({}'.format(
                node_name, ','.join(resources_node_names))

            ncores = len(jalloc_node[jalloc_node.index('[') + 1:-1].split(':'))
            allocated_cores += ncores
    assert len(job_nodes) == nodes_num, str(job_nodes)
    assert allocated_cores == nodes_num * cores_num, allocated_cores


    # check if hostname is in stdout in two lines
    with open(abspath(join(tmpdir, join(jobwdir_base, 'hostname.out'))), 'rt') as stdout_file:
        stdout_content = [line.rstrip() for line in stdout_file.readlines()]
    assert len(stdout_content) == nodes_num * cores_num, str(stdout_content)
    assert all(hostname in job_nodes for hostname in stdout_content), str(stdout_content)

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=tmpdir, dest_state='SUCCEED')

    rmtree(tmpdir)


def test_slurmenv_many_nodes_no_cores():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'hostname'
    jobwdir_base = 'hostname.sandbox'
    nodes_num = 2
    jobs = [job.to_dict() for job in [
        Job(jobName,
            JobExecution(
                exec = 'mpirun',
                args = [ '--allow-run-as-root', 'hostname' ],
                wd = abspath(join(tmpdir, jobwdir_base)),
                stdout = 'hostname.out',
                stderr = 'hostname.err',
                modules = [ 'mpi/openmpi-x86_64' ]
            ),
            JobResources(numNodes=ResourceSize(nodes_num))
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    jobEntries = check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='SUCCEED')
    assert all((isdir(abspath(join(tmpdir, jobwdir_base))),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.err')),
                stat(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')).st_size > 0))

    job_nodes = []
    allocated_cores = 0
    for jname, jentry in jobEntries.items():
        assert all(('runtime' in jentry, 'allocation' in jentry.get('runtime', {})))

        jalloc = jentry['runtime']['allocation']
        for jalloc_node in jalloc.split(','):
            node_name = jalloc_node[:jalloc_node.index('[')]
            job_nodes.append(node_name)
            print('{} in available nodes ({})'.format(node_name, ','.join(resources_node_names)))
            assert node_name in resources_node_names, '{} not in nodes ({}'.format(
                node_name, ','.join(resources_node_names))

            ncores = len(jalloc_node[jalloc_node.index('[') + 1:-1].split(':'))
            allocated_cores += ncores
    assert len(job_nodes) == nodes_num, str(job_nodes)
    assert allocated_cores > nodes_num, allocated_cores

    # check if hostname is in stdout in two lines
    with open(abspath(join(tmpdir, join(jobwdir_base, 'hostname.out'))), 'rt') as stdout_file:
        stdout_content = [line.rstrip() for line in stdout_file.readlines()]
    assert len(stdout_content) == allocated_cores, str(stdout_content)
    assert all(hostname in job_nodes for hostname in stdout_content), str(stdout_content)

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=tmpdir, dest_state='SUCCEED')

    rmtree(tmpdir)


def test_slurmenv_many_nodes_many_cores():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'hostname'
    jobwdir_base = 'hostname.sandbox'
    cores_num = resources.nodes[0].free
    nodes_num = resources.total_nodes
    jobs = [job.to_dict() for job in [
        Job(jobName,
            JobExecution(
                exec = 'mpirun',
                args = [ '--allow-run-as-root', 'hostname' ],
                wd = abspath(join(tmpdir, jobwdir_base)),
                stdout = 'hostname.out',
                stderr = 'hostname.err',
                modules = [ 'mpi/openmpi-x86_64' ]
            ),
            JobResources(numCores=ResourceSize(cores_num), numNodes=ResourceSize(nodes_num))
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    jobEntries = check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='SUCCEED')
    assert all((isdir(abspath(join(tmpdir, jobwdir_base))),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')),
                exists(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.err')),
                stat(join(abspath(join(tmpdir, jobwdir_base)), 'hostname.out')).st_size > 0))

    job_nodes = []
    allocated_cores = 0
    for jname, jentry in jobEntries.items():
        assert all(('runtime' in jentry, 'allocation' in jentry.get('runtime', {})))

        jalloc = jentry['runtime']['allocation']
        for jalloc_node in jalloc.split(','):
            node_name = jalloc_node[:jalloc_node.index('[')]
            job_nodes.append(node_name)
            print('{} in available nodes ({})'.format(node_name, ','.join(resources_node_names)))
            assert node_name in resources_node_names, '{} not in nodes ({}'.format(
                node_name, ','.join(resources_node_names))

            ncores = len(jalloc_node[jalloc_node.index('[') + 1:-1].split(':'))
            print('#{} cores on node {}'.format(ncores, node_name))
            allocated_cores += ncores
    assert len(job_nodes) == nodes_num, str(job_nodes)
    assert allocated_cores == nodes_num * cores_num, allocated_cores


    # check if hostname is in stdout in two lines
    with open(abspath(join(tmpdir, join(jobwdir_base, 'hostname.out'))), 'rt') as stdout_file:
        stdout_content = [line.rstrip() for line in stdout_file.readlines()]
    assert len(stdout_content) == nodes_num * cores_num, str(stdout_content)
    assert all(hostname in job_nodes for hostname in stdout_content), str(stdout_content)

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=tmpdir, dest_state='SUCCEED')

    rmtree(tmpdir)


def test_slurmenv_launcher_agents():
    pytest.skip('somehow this test doesn\t properly close event loop')

    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    #    with tempfile.TemporaryDirectory(dir=SHARED_PATH) as tmpdir:
    set_pythonpath_to_qcg_module()
    tmpdir = tempfile.mkdtemp(dir=SHARED_PATH)
    print('tmpdir: {}'.format(tmpdir))

    try:
        auxdir = join(tmpdir, 'qcg')

        print("aux directory set to: {}".format(auxdir))
        mkdir(auxdir)

        if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())

        LauncherExecutionJob.start_agents(tmpdir, auxdir, resources.nodes, resources.binding)

        try:
            assert len(LauncherExecutionJob.launcher.agents) == resources.total_nodes

            # there should be only one launcher per node, so check based on 'node' in agent['data']['slurm']
            node_names = set(node.name for node in resources.nodes)
            for agent_name, agent in LauncherExecutionJob.launcher.agents.items():
                print("found agent {}: {}".format(agent_name, str(agent)))
                assert all(('process' in agent, 'data' in agent, 'options' in agent.get('data', {}))), str(agent)
                assert all(('slurm' in agent['data'], 'node' in agent.get('data', {}).get('slurm', {}))), str(agent)
                assert agent['data']['slurm']['node'] in node_names
                assert all(('binding' in agent['data']['options'], agent['data']['options']['binding'] == True)), str(agent)

                node_names.remove(agent['data']['slurm']['node'])

            assert len(node_names) == 0

            # launching once more should raise exception
            with pytest.raises(Exception):
                LauncherExecutionJob.start_agents(tmpdir, auxdir, resources.nodes, resources.binding)

        finally:
            asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(LauncherExecutionJob.stop_agents()))
            time.sleep(1)

            asyncio.get_event_loop().close()
    finally:
        rmtree(tmpdir)
        pass


