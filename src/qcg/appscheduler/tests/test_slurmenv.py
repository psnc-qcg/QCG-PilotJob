import pytest
from os import environ, mkdir, stat
from os.path import dirname, join, isdir, exists, abspath
import tempfile
import logging
import asyncio
import time
from shutil import rmtree
import sys
from subprocess import run, PIPE, Popen

import qcg
from qcg.appscheduler.service import QCGPMService
from qcg.appscheduler.slurmres import in_slurm_allocation, get_num_slurm_nodes
from qcg.appscheduler.parseres import get_resources
from qcg.appscheduler.resources import ResourcesType
from qcg.appscheduler.executionjob import LauncherExecutionJob
from qcg.appscheduler.joblist import Job, JobExecution, JobResources, ResourceSize, JobDependencies
from qcg.appscheduler.tests.utils import save_reqs_to_file, check_job_status_in_json

SHARED_PATH = '/data'


def get_allocation_data():
    slurm_job_id = environ.get('SLURM_JOB_ID', None)
    print('slurm job id: {}'.format(slurm_job_id))
    out_p = run(['scontrol', 'show', 'job', slurm_job_id], shell=False, stdout=PIPE, stderr=PIPE)
    out_p.check_returncode()

    raw_data = bytes.decode(out_p.stdout).replace('\n', ' ')
    result = {}
    for element in raw_data.split(' '):
        elements = element.split('=', 1)
        result[elements[0]] = elements[1] if len(elements) > 1 else None

    return result

def set_pythonpath_to_qcg_module():
    # set PYTHONPATH to test sources
    qcg_module_path = dirname(dirname(qcg.__file__))
    print("path to the qcg.appscheduler module: {}".format(qcg_module_path))

    # in case where qcg.appscheduler are not installed in library, we must set PYTHONPATH to run a
    # launcher agnets on other nodes
    if environ.get('PYTHONPATH', None):
        environ['PYTHONPATH'] = ':'.join([environ['PYTHONPATH'], qcg_module_path])
    else:
        environ['PYTHONPATH'] = qcg_module_path


def get_slurm_resources():
    # get # of nodes from allocation
    allocation = get_allocation_data()

    # default config
    config = { }

    resources = get_resources(config)
    assert not resources is None
    assert all((resources.rtype == ResourcesType.SLURM,
                resources.totalNodes == int(allocation.get('NumNodes', '-1')),
                resources.totalCores == int(allocation.get('NumCPUs', '-1')))), \
        'resources: {}, allocation: {}'.format(str(resources), str(allocation))

    assert all((resources.freeCores == resources.totalCores,
                resources.usedCores == 0,
                resources.totalNodes == len(resources.nodes)))

    return resources, allocation

def get_slurm_resources_binded():
    resources, allocation = get_slurm_resources()

    assert resources.binding
    return resources, allocation


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


def test_slurmenv_launcher_agents():
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

        LauncherExecutionJob.StartAgents(tmpdir, auxdir, resources.nodes, resources.binding)

        try:
            assert len(LauncherExecutionJob.launcher.agents) == resources.totalNodes

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
                LauncherExecutionJob.StartAgents(tmpdir, auxdir, resources.nodes, resources.binding)

        finally:
            asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(LauncherExecutionJob.StopAgents()))
            time.sleep(2)

            tasks = asyncio.Task.all_tasks(asyncio.get_event_loop())
            print('#{} all tasks in event loop before closing'.format(len(tasks)))
            for idx, task in enumerate(tasks):
                print('\ttask {}: {}'.format(idx, str(task)))

            tasks = asyncio.Task.current_task(asyncio.get_event_loop())
            if tasks:
                logging.info('#{} current tasks in event loop before closing after waiting'.format(len(tasks)))
                for idx, task in enumerate(tasks):
                    logging.info('\ttask {}: {}'.format(idx, str(task)))

            asyncio.get_event_loop().close()
    finally:
#        rmtree(tmpdir)
        pass


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
    jobs = [job.toDict() for job in [
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

#    rmtree(tmpdir)


def test_slurmenv_exceed_nodes():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')


    resources, allocation = get_slurm_resources_binded()
    resources_node_names = set(n.name for n in resources.nodes)

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    file_path = join(tmpdir, 'jobs.json')
    print('tmpdir: {}'.format(tmpdir))

    jobName = 'mdate_failed'
    jobs = [job.toDict() for job in [
        Job(jobName,
            JobExecution(
                'date',
                wd = abspath(join(tmpdir, 'date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1), numNodes=resources.totalNodes + 1 )
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

#    print('get_event_loop is None ? {}'.format("no" if asyncio.get_event_loop() else "yes"))
#    print('get_event_loop.is_closed() ? {}'.format("yes" if asyncio.get_event_loop().is_closed() else "no"))
#    new_loop = asyncio.new_event_loop()
#    print('new event loop ? {}'.format("yes" if new_loop else "no"))
#    print('new event loop.is_closed() ? {}'.format("yes" if new_loop.is_closed() else "no"))

#    if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
#        asyncio.set_event_loop(new_loop)
    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--wd', tmpdir,
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([ jobName ], workdir=tmpdir, dest_state='FAILED')
    assert not isdir(abspath(join(tmpdir, 'date.sandbox')))

#    rmtree(tmpdir)


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
    jobs = [job.toDict() for job in [
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

#    rmtree(tmpdir)

