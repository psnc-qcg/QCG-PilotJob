from os.path import join, abspath, exists
from os import listdir, mkdir
from shutil import rmtree
import time
import pytest

from qcg.pilotjob.api.errors import ServiceError
from qcg.pilotjob.api.manager import LocalManager
from qcg.pilotjob.api.job import Jobs
from qcg.pilotjob.utils.auxdir import find_single_aux_dir

from qcg.pilotjob.tests.utils import submit_2_manager_and_wait_4_info


def test_resume_failed(tmpdir):
    non_existing_path = 'some-non-existing-directory'
    with pytest.raises(ServiceError, match=r".*Resume directory.*not exists or is not valid QCG-PilotJob auxiliary directory.*"):
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', '4', '--resume',
                          non_existing_path], {'wdir': str(tmpdir)})

    non_existing_path = join(tmpdir, 'non-pilotjob-dir')
    mkdir(non_existing_path)
    with pytest.raises(ServiceError, match=r".*Resume directory.*not exists or is not valid QCG-PilotJob auxiliary directory.*"):
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', '4', '--resume',
                          non_existing_path], {'wdir': str(tmpdir)})

def test_resume_tracker_files(tmpdir):
    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', '4'], {'wdir': str(tmpdir)})

        job_req = {
                   'name': 'host',
                   'execution': {
                       'exec': '/bin/date',
                       'stdout': 'out',
                   },
                   'resources': { 'numCores': { 'exact': 1 } }
                   }
        jobs = Jobs().add_std(job_req)
        submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')

        time.sleep(1)
        aux_dir = find_single_aux_dir(str(tmpdir))

        print(f'aux_dir content: {str(listdir(aux_dir))}')

        assert all(exists(join(aux_dir, fname)) for fname in ['track.reqs', 'track.states']), \
            f"missing tracker files in {aux_dir}: {str(listdir(aux_dir))}"

    finally:
        if m:
            m.finish()
            m.cleanup()

    rmtree(tmpdir)


def test_resume_simple(tmpdir):
    try:
        ncores = 4
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', str(ncores)],
                         {'wdir': str(tmpdir)})

        its = 10
        job_req = {
            'name': 'sleep',
            'execution': {
                'exec': '/bin/sleep',
                'args': [ '4s' ],
                'stdout': 'out',
            },
            'iteration': { 'stop': its },
            'resources': { 'numCores': { 'exact': 1 } }
        }
        jobs = Jobs().add_std(job_req)
        job_ids = m.submit(jobs)

        # because job iterations executes in order, after finish of 4th iteration, the three previous should also finish
        m.wait4('sleep:3')
        jinfos = m.info_parsed(job_ids, withChilds=True)
        assert jinfos
        jinfo = jinfos['sleep']

        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == ncores, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            exp_status = ['SUCCEED']
            if iteration > 3:
                exp_status = ['EXECUTING', 'SCHEDULED', 'QUEUED']
            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('sleep', iteration),
                        job_it.status in exp_status)),\
                f"{job_it.iteration} != {iteration}, {job_it.name} != {'{}:{}'.format('sleep', iteration)}, {job_it.status} != {exp_status}"

        # kill process
        m.kill_manager_process()
        m.cleanup()

        ncores = 4
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', str(ncores),
                          '--resume', tmpdir],
                         {'wdir': str(tmpdir)})

        m.wait4all()
        jinfos = m.info_parsed(job_ids, withChilds=True)
        assert jinfos
        jinfo = jinfos['sleep']

        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('sleep', iteration),
                        job_it.status == 'SUCCEED')), \
                f"{job_it.iteration} != {iteration}, {job_it.name} != {'{}:{}'.format('sleep', iteration)}, {job_it.status} != SUCCEED"
    finally:
        if m:
            m.finish()
            m.cleanup()

#    rmtree(tmpdir)


def test_resume_wflow(tmpdir):
    try:
        ncores = 4
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', str(ncores)],
                         {'wdir': str(tmpdir)})

        its = 10
        job_req_first = {
            'name': 'first',
            'execution': {
                'exec': '/bin/sleep',
                'args': [ '4s' ],
                'stdout': 'sleep.${it}.out',
            },
            'iteration': { 'stop': its },
            'resources': { 'numCores': { 'exact': 1 } }
        }
        job_req_second = {
            'name': 'second',
            'execution': {
                'exec': '/bin/date',
                'stdout': 'date.${it}.out',
            },
            'iteration': { 'stop': its },
            'dependencies': { 'after': [ 'first' ] },
            'resources': { 'numCores': { 'exact': 1 } }
        }
        jobs = Jobs()
        jobs.add_std(job_req_first)
        jobs.add_std(job_req_second)
        job_ids = m.submit(jobs)

        # because job iterations executes in order, after finish of 4th iteration, the three previous should also finish
        m.wait4('first:3')
        jinfos = m.info_parsed(job_ids, withChilds=True)
        assert jinfos
        jinfo = jinfos['first']

        # only first 4 iterations should finish
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == ncores, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            exp_status = ['SUCCEED']
            if iteration > 3:
                exp_status = ['EXECUTING', 'SCHEDULED', 'QUEUED']
            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('first', iteration),
                        job_it.status in exp_status)), \
                f"{job_it.iteration} != {iteration}, {job_it.name} != {'{}:{}'.format('first', iteration)}, {job_it.status} != {exp_status}"

        # none of 'second' iterations should execute
        jinfo = jinfos['second']
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == 0, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            exp_status = ['QUEUED']
            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('second', iteration),
                        job_it.status in exp_status)), \
                f"{job_it.iteration} != {iteration}, {job_it.name} != {'{}:{}'.format('second', iteration)}, {job_it.status} != {exp_status}"

        # kill process
        m.kill_manager_process()
        m.cleanup()

        ncores = 4
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', str(ncores),
                          '--resume', tmpdir], {'wdir': str(tmpdir)})

        m.wait4all()
        jinfos = m.info_parsed(job_ids, withChilds=True)
        assert jinfos

        # all iterations of 'first' job should finish
        jinfo = jinfos['first']
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('first', iteration),
                        job_it.status == 'SUCCEED')), \
                f"{job_it.iteration} != {iteration}, {job_it.name} != {'{}:{}'.format('first', iteration)}, {job_it.status} != SUCCEED"

        # all iterations of 'second' job should finish
        jinfo = jinfos['second']
        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('second', iteration),
                        job_it.status == 'SUCCEED')), \
                f"{job_it.iteration} != {iteration}, {job_it.name} != {'{}:{}'.format('sleep', iteration)}, {job_it.status} != SUCCEED"

    finally:
        if m:
            m.finish()
            m.cleanup()

    rmtree(tmpdir)
