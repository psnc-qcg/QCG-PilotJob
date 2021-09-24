import pytest
import tempfile

from os.path import join, abspath, exists
from shutil import rmtree
from pathlib import Path
from time import sleep

from qcg.pilotjob.slurmres import in_slurm_allocation, get_num_slurm_nodes

from qcg.pilotjob.tests.utils import get_slurm_resources_binded, set_pythonpath_to_qcg_module, find_single_aux_dir
from qcg.pilotjob.api.manager import LocalManager
from qcg.pilotjob.api.job import Jobs
from qcg.pilotjob.api.errors import ConnectionError
from qcg.pilotjob.api.jobinfo import JobInfo
from qcg.pilotjob.executionjob import ExecutionJob

from qcg.pilotjob.tests.utils import SHARED_PATH, submit_2_manager_and_wait_4_info


def test_slurmenv_api_resources():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})
        api_res = m.resources()

        assert all(('total_nodes' in api_res, 'total_cores' in api_res))
        assert all((api_res['total_nodes'] == resources.total_nodes, api_res['total_cores'] == resources.total_cores))

        aux_dir = find_single_aux_dir(str(tmpdir))

        assert all((exists(join(tmpdir, '.qcgpjm-client', 'api.log')),
                    exists(join(aux_dir, 'service.log'))))

    finally:
        if m:
            m.finish()
            # stopManager is using 'terminate' method on service process, which is not a best option when using
            # pytest and gathering code coverage
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_submit_simple():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs().\
            add_std({ 'name': 'host',
                     'execution': {
                         'exec': '/bin/hostname',
                         'args': [ '--fqdn' ],
                         'stdout': 'std.out',
                         'stderr': 'std.err'
                     }})
        assert submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')
    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_submit_many_cores():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs(). \
            add_std({ 'name': 'host',
                     'execution': {
                         'exec': '/bin/hostname',
                         'args': [ '--fqdn' ],
                         'stdout': 'out',
                     },
                     'resources': { 'numCores': { 'exact': resources.total_cores } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')

        # check working directories of job's inside working directory of service
        assert tmpdir == jinfos['host'].wdir, str(jinfos['host'].wdir)
        assert all((len(jinfos['host'].nodes) == resources.total_nodes,
                    jinfos['host'].total_cores == resources.total_cores)), str(jinfos['host'])

    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_submit_resource_ranges():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs(). \
            add_std({ 'name': 'host',
                     'execution': {
                         'exec': '/bin/hostname',
                         'args': [ '--fqdn' ],
                         'stdout': 'out',
                     },
                     'resources': { 'numCores': { 'min': 1 } }
                     })
        # job should faile because of missing 'max' parameter
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'FAILED')
        jinfo = jinfos['host']
        assert "Both core's range boundaries (min, max) must be defined" in jinfo.messages, str(jinfo)

        jobs = Jobs(). \
            add_std({ 'name': 'host2',
                     'execution': {
                         'exec': '/bin/hostname',
                         'args': [ '--fqdn' ],
                         'stdout': 'out',
                     },
                     'resources': {
                         'numNodes': { 'exact': 1 },
                         'numCores': { 'min': 1, 'max': resources.nodes[0].total + 1 } }
                     })
        # job should run on single node (the first free) with all available cores
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')
        jinfo = jinfos['host2']
        assert all((len(jinfo.nodes) == 1, jinfo.total_cores == resources.nodes[0].total)), str(jinfo)

        jobs = Jobs(). \
            add_std({ 'name': 'host3',
                     'execution': {
                         'exec': '/bin/hostname',
                         'args': [ '--fqdn' ],
                         'stdout': 'out',
                     },
                     'resources': {
                         'numCores': { 'min': 1, 'max': resources.nodes[0].total + 1 } }
                     })
        # job should run on at least two nodes with total maximum given cores
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')
        jinfo = jinfos['host3']
        assert all((len(jinfo.nodes) == 2, jinfo.total_cores == resources.nodes[0].total + 1)), str(jinfo)

    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_submit_exceed_total_cores():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs(). \
            add_std({ 'name': 'date',
                     'execution': { 'exec': '/bin/date' },
                     'resources': {
                         'numCores': { 'exact': resources.total_cores + 1 }
                     }})
        with pytest.raises(ConnectionError, match=r".*Not enough resources.*"):
            m.submit(jobs)
        assert len(m.list()) == 0

        jobs = Jobs(). \
        add_std({ 'name': 'date',
                     'execution': { 'exec': '/bin/date' },
                     'resources': {
                         'numNodes': { 'exact': resources.total_nodes + 1 }
                     }})
        with pytest.raises(ConnectionError, match=r".*Not enough resources.*"):
            ids = m.submit(jobs)
        assert len(m.list()) == 0

        jobs = Jobs(). \
            add_std({ 'name': 'date',
                     'execution': {
                         'exec': '/bin/date',
                         'stdout': 'std.out',
                     },
                     'resources': { 'numCores': { 'exact': resources.total_cores  } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')
        assert jinfos['date'].total_cores == resources.total_cores
    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_std_streams():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs(). \
            add_std({ 'name': 'host',
                     'execution': {
                         'exec': 'cat',
                         'stdin': '/etc/system-release',
                         'stdout': 'out',
                         'stderr': 'err'
                     }})
        assert submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')

        assert all((exists(join(tmpdir, 'out')), exists(join(tmpdir, 'err'))))

        with open(join(tmpdir, 'out'), 'rt') as out_f:
            out = out_f.read()

        with open(join('/etc/system-release'), 'rt') as sr_f:
            system_release = sr_f.read()

        assert system_release in out
    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_std_streams_many_cores():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs(). \
            add_std({ 'name': 'host',
                     'execution': {
                         'exec': 'cat',
                         'stdin': '/etc/system-release',
                         'stdout': 'out',
                         'stderr': 'err'
                     },
                     'resources': {
                         'numCores': { 'exact': 2 }
                     }
                     })
        assert submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')

        assert all((exists(join(tmpdir, 'out')), exists(join(tmpdir, 'err'))))

        with open(join(tmpdir, 'out'), 'rt') as out_f:
            out = out_f.read()

        with open(join('/etc/system-release'), 'rt') as sr_f:
            system_release = sr_f.read()

        assert system_release in out
    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_iteration_simple():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        its = 2
        jobs = Jobs(). \
            add_std({ 'name': 'host',
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'hostname', 'args': [ '--fqdn' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'exact': 1 } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')
        assert jinfos
        jinfo = jinfos['host']
        print('jinfo: {}'.format(jinfo))
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0))

        its = 2
        jobs = Jobs(). \
            add_std({ 'name': 'host2',
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'hostname', 'args': [ '--fqdn' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'exact': 1 } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos['host2']
        print('jinfo: {}'.format(jinfo))
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0))
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format('host2', iteration),
                        job_it.wdir == tmpdir, job_it.total_cores == 1))
    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_iteration_core_scheduling():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        # in that case the 'split-into' is default the number of iterations
        # so total available resources should be splited into two partitions and each of the
        # iteration should run on its own partition
        jname = 'host'
        its = 2
        jobs = Jobs(). \
           add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'hostname', 'args': [ '--fqdn' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'split-into' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        # all iterations has been scheduled across all resources
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores
        assert all(child.total_cores == resources.total_cores / its for child in jinfo.childs)

        # we explicity specify the 'split-into' parameter to 2, behavior should be the same as in the
        # previous example
        jname = 'host2'
        its = 2
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'hostname', 'args': [ '--fqdn' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'split-into', 'params': { 'parts': 2 } } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        # all iterations has been scheduled across all resources
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores
        assert all(child.total_cores == resources.total_cores / 2 for child in jinfo.childs)

        # we explicity specify the 'split-into' parameter to 4, the two iterations should be sheduled
        # on half of the available resources
        jname = 'host3'
        its = 2
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'hostname', 'args': [ '--fqdn' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'split-into', 'params': { 'parts': 4 } } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        # all iterations has been scheduled across all resources
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores / 2
        assert all(child.total_cores == resources.total_cores / 4 for child in jinfo.childs)

        # we explicity specify the 'split-into' parameter to 2, but the number of iterations is larger than
        # available partitions in the same time, so they should be executed serially (by parts)
        jname = 'host4'
        its = 10
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'hostname', 'args': [ '--fqdn' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'split-into', 'params': { 'parts': 2 } } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        assert all(child.total_cores == resources.total_cores / 2 for child in jinfo.childs)

        # the 'maximum-iters' scheduler is trying to launch as many iterations in the same time on all available
        # resources
        jname = 'host5'
        its = 2
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores

        # the 'maximum-iters' scheduler is trying to launch as many iterations in the same time on all available
        # resources
        jname = 'host6'
        its = resources.total_cores
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores

        # in case where number of iterations exceeds the number of available resources, the 'maximum-iters' schedulers
        # splits iterations into 'steps' minimizing this number, and allocates as many resources as possible for each
        # iteration inside 'step'
        jname = 'host7'
        its = resources.total_cores
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        assert (child.total_cores == 1 for child in jinfo.childs)
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores

        # in case where number of iterations exceeds the number of available resources, the 'maximum-iters' schedulers
        # splits iterations into 'steps' minimizing this number, and allocates as many resources as possible for each
        # iteration inside 'step'
        jname = 'host8'
        its = resources.total_cores * 2
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1, job_it.total_cores < resources.total_cores)), str(job_it)
        assert (child.total_cores == 1 for child in jinfo.childs)
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores * 2

        # in case where number of iterations exceeds the number of available resources, the 'maximum-iters' schedulers
        # splits iterations into 'steps' minimizing this number, and allocates as many resources as possible for each
        # iteration inside 'step'
        jname = 'host9'
        its = resources.total_cores + 1
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores >= 1)), str(job_it)
        assert (child.total_cores == 1 for child in jinfo.childs)
        # because all iterations will be splited in two 'steps' and in each step the iterations that has been assigned
        # for the step should usage maximum available resources
        assert sum([ child.total_cores for child in jinfo.childs ]) == resources.total_cores * 2


        # in this case where two iterations can't fit at once on resources, all the iterations should be scheduled
        # serially on all available resources
        jname = 'host10'
        its = resources.total_nodes
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'min': resources.total_cores - 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores == resources.total_cores, len(job_it.nodes) == resources.total_nodes)),\
                str(job_it)
    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_iteration_node_scheduling():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    # TODO: it's hard to write comprehensive iteration scheduling node tests on only two nodes (in slurm's \
    #  development docker)

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        # in that case the 'split-into' is default the number of iterations
        # so total available resources should be splited into two partitions and each of the
        # iteration should run on its own partition
        jname = 'host'
        its = 2
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out_${it}', 'stderr': 'err_${it}' },
                     'resources': { 'numCores': { 'exact': resources.nodes[0].total },
                                    'numNodes': { 'min': 1,
                                                  'scheduler': { 'name': 'split-into' } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores == resources.nodes[0].total, len(job_it.nodes) == 1)), str(job_it)
        # all iterations has been scheduled across all nodes
        assert sum([ len(child.nodes) for child in jinfo.childs ]) == resources.total_nodes
        # the iterations should execute on different node
        assert list(jinfo.childs[0].nodes)[0] != list(jinfo.childs[1].nodes)[0]

        # we explicity specify the 'split-into' parameter to 2, behavior should be the same as in the
        # previous example
        jname = 'host2'
        its = 2
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'exact': resources.nodes[0].total },
                                    'numNodes': { 'min': 1,
                                                  'scheduler': { 'name': 'split-into', 'params': { 'parts': 2 } } } }
                     })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores == resources.nodes[0].total, len(job_it.nodes) == 1)), str(job_it)
        # all iterations has been scheduled across all nodes
        assert sum([ len(child.nodes) for child in jinfo.childs ]) == resources.total_nodes
        # the iterations should execute on different node
        assert list(jinfo.childs[0].nodes)[0] != list(jinfo.childs[1].nodes)[0]

        # the 'maximum-iters' scheduler is trying to launch as many iterations in the same time on all available
        # resources
        jname = 'host3'
        its = 4
        jobs = Jobs(). \
            add_std({ 'name': jname,
                     'iteration': { 'stop': its },
                     'execution': { 'exec': 'sleep', 'args': [ '2s' ], 'stdout': 'out' },
                     'resources': { 'numCores': { 'exact': resources.nodes[0].total },
                                    'numNodes': { 'min': 1,
                                                  'scheduler': { 'name': 'maximum-iters' } } }
         })
        jinfos = submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED', withChilds=True)
        assert jinfos
        jinfo = jinfos[jname]
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]
            print('job iteration {}: {}'.format(iteration, str(job_it)))
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jname, iteration),
                        job_it.total_cores == resources.nodes[0].total, len(job_it.nodes) == 1)), str(job_it)
        assert sum([len(child.nodes) for child in jinfo.childs]) == its

    finally:
        if m:
            m.finish()
#            m.stopManager()
            m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_cancel_nl():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))
    print(f'tmpdir: {tmpdir}')

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        iters=10
        ids = m.submit(Jobs().
                       add(exec='/bin/sleep', args=['5s'], iteration=iters, stdout='sleep.out.${it}',
                           stderr='sleep.err.${it}', numCores=1)
                       )
        jid = ids[0]
        assert len(m.list()) == 1

        list_jid = list(m.list().keys())[0]
        assert list_jid == jid

        # wait for job to start executing
        sleep(2)

        m.cancel([jid])

        m.wait4(m.list())

        jinfos = m.info_parsed(ids, withChilds=True)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'CANCELED'))

        # the canceled iterations are included in 'failed' entry in job statistics
        # the cancel status is presented in 'childs/state' entry
        assert all((jinfos[jid].iterations, jinfos[jid].iterations.get('start', -1) == 0,
                    jinfos[jid].iterations.get('stop', 0) == iters, jinfos[jid].iterations.get('total', 0) == iters,
                    jinfos[jid].iterations.get('finished', 0) == iters, jinfos[jid].iterations.get('failed', -1) == iters))
        assert len(jinfos[jid].childs) == iters
        for iteration in range(iters):
            job_it = jinfos[jid].childs[iteration]
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jid, iteration),
                        job_it.status == 'CANCELED')), str(job_it)

        m.remove(jid)

    finally:
        m.finish()
        m.cleanup()

    rmtree(tmpdir)


def test_slurmenv_api_cancel_kill_nl():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))
    print(f'tmpdir: {tmpdir}')

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        iters=10
        ids = m.submit(Jobs().
                       add(script='trap "" SIGTERM; sleep 30s', iteration=iters, stdout='sleep.out.${it}',
                           stderr='sleep.err.${it}', numCores=1)
                       )
        jid = ids[0]
        assert len(m.list()) == 1

        list_jid = list(m.list().keys())[0]
        assert list_jid == jid

        # wait for job to start executing
        sleep(2)

        m.cancel([jid])

        # wait for SIGTERM job cancel
        sleep(2)

        jinfos = m.info_parsed(ids)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'QUEUED'))

        # wait for SIGKILL job cancel (~ExecutionJob.SIG_KILL_TIMEOUT)
        sleep(ExecutionJob.SIG_KILL_TIMEOUT)

        jinfos = m.info_parsed(ids, withChilds=True)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'CANCELED'))

        # the canceled iterations are included in 'failed' entry in job statistics
        # the cancel status is presented in 'childs/state' entry
        assert all((jinfos[jid].iterations, jinfos[jid].iterations.get('start', -1) == 0,
                    jinfos[jid].iterations.get('stop', 0) == iters, jinfos[jid].iterations.get('total', 0) == iters,
                    jinfos[jid].iterations.get('finished', 0) == iters, jinfos[jid].iterations.get('failed', -1) == iters))
        assert len(jinfos[jid].childs) == iters
        for iteration in range(iters):
            job_it = jinfos[jid].childs[iteration]
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jid, iteration),
                        job_it.status == 'CANCELED')), str(job_it)

        m.remove(jid)

    finally:
        m.finish()
        m.cleanup()

#    rmtree(tmpdir)

