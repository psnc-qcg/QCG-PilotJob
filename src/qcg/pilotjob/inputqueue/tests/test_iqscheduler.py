from qcg.pilotjob.inputqueue.iqmanager import IQManager
from qcg.pilotjob.inputqueue.iqhandler import IQHandler
from qcg.pilotjob.response import ResponseCode
from qcg.pilotjob.inputqueue.jobdb import job_db
from qcg.pilotjob.inputqueue.job import JobState


def test_iqscheduler_submit_nodeps():
    """Non iteration job submitted without dependencies"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 1,
                len(response.data.get('jobs', [])) == 1, 'job_01' in response.data.get('jobs', []))), str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0,
                'job_01' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_01'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_01'].iterations))


def test_iqscheduler_submit_deps_simple():
    """Non iteration job submitted with dependencies"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'dependencies': {'after': ['job_02']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 2,
                len(response.data.get('jobs', [])) == 2)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02']), str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 1,
                'job_02' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_02'].iterations))
    assert all(('job_02' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_jobs))

    job_db.get('job_02').change_state(JobState.SUCCEED)
    iq_manager.job_finished('job_02')

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0,
                'job_01' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_01'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_01'].iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_02'].iterations))
    assert all((iq_manager.ready_iterations['job_01'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None))

def test_iqscheduler_submit_iters():
    """Iteration job submitted"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 1,
                len(response.data.get('jobs', [])) == 1, 'job_01' in response.data.get('jobs', []))), str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0,
                'job_01' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_01'].iterations) == it_stop - it_start))
    assert all(idx in iq_manager.ready_iterations['job_01'].iterations for idx in range(it_stop - it_start))


def test_iqscheduler_submit_deps_iters():
    """Specific iteration dependencies"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}}
    ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 2,
                len(response.data.get('jobs', [])) == 2)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02']), str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 1,
                'job_02' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert all(('job_02' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                iq_manager.non_ready_jobs['job_01'].iterations_deps_mask is not None,
                iq_manager.non_ready_jobs['job_01'].iterations_deps_count is not None,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 0,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 1,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == (1 if left_its else 0),
                    len(iq_manager.tracked_jobs) == (1 if left_its else 0),
                    'job_02' in iq_manager.tracked_jobs if left_its else True,
                    'job_01' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations,
                    len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start))
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == it_nr + 1,\
            str(iq_manager.ready_iterations['job_01'].iterations)
        assert all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_nr + 1))
        assert all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start))

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0,
                'job_01' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_01'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start))
    assert all(idx in iq_manager.ready_iterations['job_01'].iterations for idx in range(it_stop - it_start))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[idx] == 0
               for idx in range(it_stop - it_start))


def test_iqscheduler_submit_deps_many_iters():
    """Specific iteration dependencies and job dependencies (specific iteration dependencies changed first)"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}', 'job_03']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}},
                       {'name': 'job_03',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 3,
                len(response.data.get('jobs', [])) == 3)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02', 'job_03']),\
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 2,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_03'].iterations) == 1))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert 0 in iq_manager.ready_iterations['job_03'].iterations
    assert all(('job_02' in iq_manager.tracked_jobs,
                'job_03' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                len(iq_manager.tracked_jobs['job_03']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01'],
                iq_manager.tracked_jobs['job_03'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 1,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 1,
                'job_03' in iq_manager.non_ready_jobs['job_01'].after_jobs,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == 1,
                    len(iq_manager.tracked_jobs) == (2 if left_its else 1),
                    'job_02' in iq_manager.tracked_jobs if left_its else True,
                    'job_03' in iq_manager.tracked_jobs)), f'{it_nr}: tracked {len(iq_manager.tracked_jobs)}'
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_03'].iterations))
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == 0
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
                   for it in range(it_nr + 1))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it_nr + 1 + it] not in [0, -1]
                   for it in range(left_its))

    job_db.get('job_03').change_state(JobState.SUCCEED)
    iq_manager.job_finished('job_03')

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_03'].iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
               for it in range(it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_01'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_03'].job.has_dependencies() is False))


def test_iqscheduler_submit_deps_many_iters_2():
    """Specific iteration dependencies and job dependencies (job dependencies changed first)"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}', 'job_03']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}},
                       {'name': 'job_03',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 3,
                len(response.data.get('jobs', [])) == 3)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02', 'job_03']), \
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 2,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_03'].iterations) == 1))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert 0 in iq_manager.ready_iterations['job_03'].iterations
    assert all(('job_02' in iq_manager.tracked_jobs,
                'job_03' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                len(iq_manager.tracked_jobs['job_03']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01'],
                iq_manager.tracked_jobs['job_03'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                iq_manager.non_ready_jobs['job_01'].iterations_deps_mask is not None,
                iq_manager.non_ready_jobs['job_01'].iterations_deps_count is not None,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 1,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 1,
                'job_03' in iq_manager.non_ready_jobs['job_01'].after_jobs,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs))

    job_db.get('job_03').change_state(JobState.SUCCEED)
    iq_manager.job_finished('job_03')

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 1))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == 0,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_03'].iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] not in [0, -1]
               for it in range(it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_01'].job.has_dependencies() is True,
                iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_03'].job.has_dependencies() is False))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == (1 if left_its else 0),
                    len(iq_manager.tracked_jobs) == (1 if left_its else 0),
                    'job_02' in iq_manager.tracked_jobs if left_its else True))
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_03'].iterations))
        assert all((len(iq_manager.ready_iterations['job_01'].iterations) == it_nr + 1,
                    all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_nr + 1))))
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
                   for it in range(it_nr + 1))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it_nr + 1 + it] not in [0, -1]
                   for it in range(left_its))

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_03'].iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
               for it in range(it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_01'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_03'].job.has_dependencies() is False))


def test_iqscheduler_submit_deps_many_iters_many():
    """Many specific iteration dependencies"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}', 'job_03:${it}', 'job_04']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}},
                       {'name': 'job_03',
                        'execution': {'exec': 'date'},
                       'iteration': {'start': it_start, 'stop': it_stop}},
                       {'name': 'job_04',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 4,
                len(response.data.get('jobs', [])) == 4)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02', 'job_03', 'job_04']), \
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 3,
                'job_02' in iq_manager.tracked_jobs,
                'job_03' in iq_manager.tracked_jobs,
                'job_04' in iq_manager.tracked_jobs))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_04' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == 0,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_03'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_04'].iterations) == 1))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert all(idx in iq_manager.ready_iterations['job_03'].iterations for idx in range(it_stop - it_start))
    assert 0 in iq_manager.ready_iterations['job_04'].iterations
    assert all((len(iq_manager.tracked_jobs['job_02']) == 1,
                len(iq_manager.tracked_jobs['job_03']) == 1,
                len(iq_manager.tracked_jobs['job_04']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01'],
                iq_manager.tracked_jobs['job_03'][0] == iq_manager.non_ready_jobs['job_01'],
                iq_manager.tracked_jobs['job_04'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 1,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 2,
                'job_04' in iq_manager.non_ready_jobs['job_01'].after_jobs,
                all(jid in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs for jid in ['job_02', 'job_03'])))

    job_db.get('job_04').change_state(JobState.SUCCEED)
    iq_manager.job_finished('job_04')

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 2))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_04' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == 0,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_03'].iterations for it in range(it_stop - it_start)),
                0 in iq_manager.ready_iterations['job_04'].iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 2,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == it_stop - it_start,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[1] == it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_04'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_04'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] not in [0, -1]
               for it in range(it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_01'].job.has_dependencies() is True,
                iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_03'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_04'].job.has_dependencies() is False))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == 1,
                    len(iq_manager.tracked_jobs) == (2 if left_its else 1),
                    'job_02' in iq_manager.tracked_jobs if left_its else True,
                    'job_03' in iq_manager.tracked_jobs))
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_04' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_03'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_04'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_04'].iterations))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == 0
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 2,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[1] == it_stop - it_start))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] not in [0, -1]
               for it in range(it_stop - it_start))

    # second change of the same iterations should nothing change
    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == 1,
                    len(iq_manager.tracked_jobs) == 1,
                    'job_03' in iq_manager.tracked_jobs))
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_04' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_03'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_04'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_04'].iterations))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == 0
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 2,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[1] == it_stop - it_start))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] not in [0, -1]
                   for it in range(it_stop - it_start))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_03').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_03', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == (1 if left_its else 0),
                    len(iq_manager.tracked_jobs) == (1 if left_its else 0),
                    'job_03' in iq_manager.tracked_jobs if left_its else True))
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_04' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_03'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_04'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_04'].iterations))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == it_nr + 1
        assert all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_nr + 1))
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 2,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[1] == left_its))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
                   for it in range(it_nr + 1))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it_nr + 1 + it] not in [0, -1]
                   for it in range(left_its))

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_04' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_03'].iterations for it in range(it_stop - it_start))))
    assert all((len(iq_manager.ready_iterations['job_04'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_04'].iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 2,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[1] == 0))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_04'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_04'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
               for it in range(it_stop - it_start))
    assert all((iq_manager.ready_iterations['job_01'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_04'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_04'].job.has_dependencies() is False))


def test_iqscheduler_submit_deps_omit_iter():
    """One of specific iteration dependencies fail"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}', 'job_03']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}},
                       {'name': 'job_03',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 3,
                len(response.data.get('jobs', [])) == 3)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02', 'job_03']), \
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 2,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_03'].iterations) == 1))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert 0 in iq_manager.ready_iterations['job_03'].iterations
    assert all(('job_02' in iq_manager.tracked_jobs,
                'job_03' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                len(iq_manager.tracked_jobs['job_03']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01'],
                iq_manager.tracked_jobs['job_03'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 1,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 1,
                'job_03' in iq_manager.non_ready_jobs['job_01'].after_jobs,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs))

    # all but last iteration succeed
    for it_nr in range(it_stop - it_start - 1):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == 1,
                    len(iq_manager.tracked_jobs) == 2,
                    'job_02' in iq_manager.tracked_jobs,
                    'job_03' in iq_manager.tracked_jobs))
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_03'].iterations))
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == 0
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
                   for it in range(it_nr + 1))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it_nr + 1 + it] not in [0, -1]
                   for it in range(left_its))

    # last iteration fail
    last_it = it_stop - it_start - 1
    job_db.get('job_02').change_state(JobState.FAILED, last_it + it_start)
    iq_manager.job_finished('job_02', iteration=last_it + it_start)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 1,
                'job_03' in iq_manager.tracked_jobs))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == 0,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_03'].iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
               for it in range(it_stop - it_start - 1))
    assert iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[last_it] == -1

    assert job_db.get('job_02').state(it_start + last_it) == JobState.FAILED
    assert job_db.get('job_01').state(it_start + last_it) == JobState.OMITTED

    job_db.get('job_03').change_state(JobState.SUCCEED)
    iq_manager.job_finished('job_03')

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0))
    assert all(('job_01' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_01'].iterations) == it_stop - it_start - 1,
                all(it in iq_manager.ready_iterations['job_01'].iterations for it in range(it_stop - it_start - 1)),
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_03'].iterations)), str(iq_manager.ready_iterations['job_01'].iterations)
    assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == 0))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
    assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
               for it in range(it_stop - it_start - 1))
    assert iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[last_it] == -1
    assert all((iq_manager.ready_iterations['job_01'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_03'].job.has_dependencies() is False))


def test_iqscheduler_submit_deps_omit():
    """All specific iteration dependencies are met but dependent job fail"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}', 'job_03']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}},
                       {'name': 'job_03',
                        'execution': {'exec': 'date'}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 3,
                len(response.data.get('jobs', [])) == 3)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02', 'job_03']), \
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 2,
                'job_02' in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                len(iq_manager.ready_iterations['job_03'].iterations) == 1))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert 0 in iq_manager.ready_iterations['job_03'].iterations
    assert all(('job_02' in iq_manager.tracked_jobs,
                'job_03' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                len(iq_manager.tracked_jobs['job_03']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01'],
                iq_manager.tracked_jobs['job_03'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 1,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 1,
                'job_03' in iq_manager.non_ready_jobs['job_01'].after_jobs,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.SUCCEED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == 1,
                    len(iq_manager.tracked_jobs) == (2 if left_its else 1),
                    'job_02' in iq_manager.tracked_jobs if left_its else True,
                    'job_03' in iq_manager.tracked_jobs))
        assert all(('job_01' in iq_manager.ready_iterations,
                    'job_03' in iq_manager.ready_iterations,
                    'job_02' in iq_manager.ready_iterations))
        assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                    all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                    len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                    0 in iq_manager.ready_iterations['job_03'].iterations))
        assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                    iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its))
        assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                    iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
        assert len(iq_manager.ready_iterations['job_01'].iterations) == 0
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == 0
                   for it in range(it_nr + 1))
        assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it_nr + 1 + it] not in [0, -1]
                   for it in range(left_its))

    job_db.get('job_03').change_state(JobState.FAILED)
    iq_manager.job_finished('job_03')

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0))
    assert all(('job_01' not in iq_manager.ready_iterations,
                'job_03' in iq_manager.ready_iterations,
                'job_02' in iq_manager.ready_iterations))
    assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start)),
                len(iq_manager.ready_iterations['job_03'].iterations) == 1,
                0 in iq_manager.ready_iterations['job_03'].iterations)), str(iq_manager.ready_iterations['job_01'].iterations)
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_03'].job.iterations_deps_count == None))
    assert all((iq_manager.ready_iterations['job_02'].job.has_dependencies() is False,
                iq_manager.ready_iterations['job_03'].job.has_dependencies() is False))

    assert job_db.get('job_01').state() == JobState.OMITTED


def test_iqscheduler_submit_deps_omit_all_iters():
    """All iterations job depends on fails"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    it_start = 2
    it_stop = 5
    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': 'job_01',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop},
                        'dependencies': {'after': ['job_02:${it}']}},
                       {'name': 'job_02',
                        'execution': {'exec': 'date'},
                        'iteration': {'start': it_start, 'stop': it_stop}}
                   ]}

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 2,
                len(response.data.get('jobs', [])) == 2)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02']), \
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 1,
                len(iq_manager.tracked_jobs) == 1,
                'job_02' in iq_manager.ready_iterations,
                len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start))
    assert all(idx in iq_manager.ready_iterations['job_02'].iterations for idx in range(it_stop - it_start))
    assert all(('job_02' in iq_manager.tracked_jobs,
                len(iq_manager.tracked_jobs['job_02']) == 1,
                iq_manager.tracked_jobs['job_02'][0] == iq_manager.non_ready_jobs['job_01']))
    assert all(('job_01' in iq_manager.non_ready_jobs,
                len(iq_manager.non_ready_jobs['job_01'].after_jobs) == 0,
                len(iq_manager.non_ready_jobs['job_01'].after_iteration_jobs) == 1,
                'job_02' in iq_manager.non_ready_jobs['job_01'].after_iteration_jobs,
                iq_manager.non_ready_jobs['job_01'].has_dependencies()))

    for it_nr in range(it_stop - it_start):
        it_idx = it_start + it_nr
        left_its = it_stop - it_start - it_nr - 1

        job_db.get('job_02').change_state(JobState.FAILED, it_idx)
        iq_manager.job_finished('job_02', iteration=it_idx)

        assert all((len(iq_manager.non_ready_jobs) == (1 if left_its else 0),
                    len(iq_manager.tracked_jobs) == (1 if left_its else 0),
                    'job_02' in iq_manager.tracked_jobs if left_its else True))
        assert all(('job_01' in iq_manager.ready_iterations if left_its else True,
                    'job_02' in iq_manager.ready_iterations))
        assert len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start
        if left_its:
            assert all((len(iq_manager.ready_iterations['job_01'].job.iterations_deps_count) == 1,
                        iq_manager.ready_iterations['job_01'].job.iterations_deps_count[0] == left_its))
            assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                        iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None))
            assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it] == -1
                       for it in range(it_nr + 1))
            assert all(iq_manager.ready_iterations['job_01'].job.iterations_deps_mask[it_nr + 1 + it] not in [0, -1]
                       for it in range(left_its))
            assert len(iq_manager.ready_iterations) == 2
            assert len(iq_manager.ready_iterations['job_01'].iterations) == 0
        else:
            assert len(iq_manager.ready_iterations) == 1

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0,
                len(iq_manager.ready_iterations) == 1))
    assert 'job_02' in iq_manager.ready_iterations
    assert all((len(iq_manager.ready_iterations['job_02'].iterations) == it_stop - it_start,
                all(it in iq_manager.ready_iterations['job_02'].iterations for it in range(it_stop - it_start))))
    assert all((iq_manager.ready_iterations['job_02'].job.iterations_deps_mask == None,
                iq_manager.ready_iterations['job_02'].job.iterations_deps_count == None))

    assert job_db.get('job_01').state() == JobState.OMITTED


def test_iqscheduler_ready_iterations():
    """Ready iterations for jobs w/o dependencies"""

    # clear database
    job_db.clear()

    iq_manager = IQManager()
    iq_receiver = IQHandler(iq_manager)

    jobs_spec = {
        'job_01': {'iters': {'start': 0, 'stop': 5}, 'resources': { 'cores': { 'exact':  1 }}},
        'job_02': {'iters': {'start': 5, 'stop': 7}, 'resources': { 'cores': { 'exact':  2 }}}
    }

    submit_req = { 'cmd': 'submit',
                   'jobs': [
                       {'name': jname,
                        'execution': {'exec': 'date'},
                        'iteration': job['iters'],
                        'resources': job['resources']
                        } for jname, job in jobs_spec.items()]
                 }

    response = iq_receiver.handle_submit_job(submit_req)
    assert all((response.code == ResponseCode.OK, response.data.get('submitted', 0) == 2,
                len(response.data.get('jobs', [])) == 2)), str(response.data)
    assert (job_name in response.data.get('jobs', []) for job_name in ['job_01', 'job_02']), \
        str(response.data)

    assert all((len(iq_manager.non_ready_jobs) == 0,
                len(iq_manager.tracked_jobs) == 0,
                len(iq_manager.ready_iterations) == 2))
    assert all(job in iq_manager.ready_iterations for job in ['job_01', 'job_02'])
    assert all(len(iq_manager.ready_iterations[job_id].iterations) == job['iters']['stop'] - job['iters']['start']
               for job_id, job in jobs_spec.items())

    allocation_1, ncores_1 = iq_manager.get_ready_iterations(4, max_cpu_cores=28, max_node_cpu_cores=28, max_nodes=1)
    assert all((allocation_1 is not None, ncores_1 == 4)), str(allocation_1)
    print(f'allocation_1 for 4 cores: {str(allocation_1)}')

    allocation_2, ncores_2 = iq_manager.get_ready_iterations(4, max_cpu_cores=28, max_node_cpu_cores=28, max_nodes=1)
    assert all((allocation_2 is not None, ncores_2 == 3)), str(allocation_2)
    print(f'allocation_2 for 4 cores: {str(allocation_2)}')

    assert len(iq_manager.ready_iterations) == 1

    allocation_3, ncores_3 = iq_manager.get_ready_iterations(4, max_cpu_cores=28, max_node_cpu_cores=28, max_nodes=1)
    assert all((allocation_3 is not None, ncores_3 == 2)), str(allocation_3)
    print(f'allocation_3 for 4 cores: {str(allocation_3)}')

    assert len(iq_manager.ready_iterations) == 0

    assert sum([ncores_1, ncores_2, ncores_3]) ==\
           sum((job['iters']['stop'] - job['iters']['start']) * job['resources']['cores']['exact']
               for job in jobs_spec.values())

