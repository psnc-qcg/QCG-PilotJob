from qcg.pilotjob.executor.queue import ScheduleQueue
from qcg.pilotjob.executor.job import JobIterations
from qcg.pilotjob.resources import Resources, ResourcesType, Node
from qcg.pilotjob.scheduler import Scheduler
from qcg.pilotjob.inputqueue.jobdesc import JobDescription

import logging

def test_queue_simple(caplog):
    caplog.set_level(logging.DEBUG)
    resources = Resources(ResourcesType.LOCAL, [Node('n1', 2), Node('n2', 2)])

    stats = { 'executed': 0, 'failed': 0 }
    allocations = list()

    def execute_it(job_iterations, iteration, allocation):
        print(f'executing iteration {job_iterations.id}:{iteration} on allocation {allocation}...')
        stats['executed'] = stats['executed'] + 1
        allocations.append(allocation)

    def failed_it(job_iterations, iteration, message):
        print(f'iteration {job_iterations.id}:{iteration} failed ...')
        stats['failed'] = stats['failed'] + 1

    d1 = JobDescription(name='job1', execution={'exec': 'date'}, resources={'cores': {'exact': 1}})
    j1_total_iterations = 10
    j1 = JobIterations('job1-id', d1, {}, list(range(j1_total_iterations)))

    queue = ScheduleQueue(Scheduler(resources), execute_it, failed_it)
    queue.enqueue([j1])
    queue.schedule_loop()

    assert all((stats['executed'] == resources.total_cores,
                stats['failed'] == 0,
                len(allocations) == resources.total_cores))

    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats['executed'] == 2 * resources.total_cores,
                stats['failed'] == 0,
                len(allocations) == resources.total_cores))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats['executed'] == j1_total_iterations,
                stats['failed'] == 0,
                len(allocations) == 2))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats['executed'] == j1_total_iterations,
                stats['failed'] == 0,
                len(allocations) == 0))


def test_queue_simple_even(caplog):
    caplog.set_level(logging.DEBUG)
    resources = Resources(ResourcesType.LOCAL, [Node('n1', 2), Node('n2', 2)])

    stats = { 'executed': 0, 'failed': 0 }
    allocations = list()

    def execute_it(job_iterations, iteration, allocation):
        print(f'executing iteration {job_iterations.id}:{iteration} on allocation {allocation}...')
        stats['executed'] = stats['executed'] + 1
        allocations.append(allocation)

    def failed_it(job_iterations, iteration, message):
        print(f'iteration {job_iterations.id}:{iteration} failed ...')
        stats['failed'] = stats['failed'] + 1

    d1 = JobDescription(name='job1', execution={'exec': 'date'}, resources={'cores': {'exact': 3}})
    j1_total_iterations = 3
    j1 = JobIterations('job1-id', d1, {}, list(range(j1_total_iterations)))

    queue = ScheduleQueue(Scheduler(resources), execute_it, failed_it)
    queue.enqueue([j1])

    queue.schedule_loop()
    assert all((stats['executed'] == 1,
                stats['failed'] == 0,
                len(allocations) == 1))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats['executed'] == 2,
                stats['failed'] == 0,
                len(allocations) == 1))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats['executed'] == 3,
                stats['failed'] == 0,
                len(allocations) == 1))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats['executed'] == 3,
                stats['failed'] == 0,
                len(allocations) == 0))


def test_queue_simple_even_many(caplog):
    caplog.set_level(logging.DEBUG)
    resources = Resources(ResourcesType.LOCAL, [Node('n1', 2), Node('n2', 3)])

    job_ids = ['job1-id', 'job2-id']
    stats = {}
    for job_id in job_ids:
        stats[job_id] = { 'executed': 0, 'failed': 0 }

    allocations = list()

    def execute_it(job_iterations, iteration, allocation):
        print(f'executing iteration {job_iterations.id}:{iteration} on allocation {allocation}...')
        stats[job_iterations.id]['executed'] = stats[job_iterations.id]['executed'] + 1
        allocations.append(allocation)

    def failed_it(job_iterations, iteration, message):
        print(f'iteration {job_iterations.id}:{iteration} failed ...')
        stats[job_iterations.id]['failed'] = stats[job_iterations.id]['failed'] + 1

    d1 = JobDescription(name='job1', execution={'exec': 'date'}, resources={'cores': {'exact': 3}})
    j1_total_iterations = 3
    j1 = JobIterations(job_ids[0], d1, {}, list(range(j1_total_iterations)))

    d2 = JobDescription(name='job2', execution={'exec': 'date'}, resources={'cores': {'exact': 1}})
    j2_total_iterations = 6
    j2 = JobIterations(job_ids[1], d2, {}, list(range(j2_total_iterations)))

    queue = ScheduleQueue(Scheduler(resources), execute_it, failed_it)
    queue.enqueue([j1, j2])

    queue.schedule_loop()
    assert all((stats[job_ids[0]]['executed'] == 1,
                stats[job_ids[0]]['failed'] == 0,
                stats[job_ids[1]]['executed'] == 2,
                stats[job_ids[1]]['failed'] == 0,
                len(allocations) == 3))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats[job_ids[0]]['executed'] == 2,
                stats[job_ids[0]]['failed'] == 0,
                stats[job_ids[1]]['executed'] == 4,
                stats[job_ids[1]]['failed'] == 0,
                len(allocations) == 3))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats[job_ids[0]]['executed'] == 3,
                stats[job_ids[0]]['failed'] == 0,
                stats[job_ids[1]]['executed'] == 6,
                stats[job_ids[1]]['failed'] == 0,
                len(allocations) == 3))
    for allocation in allocations:
        queue.scheduler.release_allocation(allocation)
    allocations.clear()

    queue.schedule_loop()
    assert all((stats[job_ids[0]]['executed'] == 3,
                stats[job_ids[0]]['failed'] == 0,
                stats[job_ids[1]]['executed'] == 6,
                stats[job_ids[1]]['failed'] == 0,
                len(allocations) == 0))


def test_queue_simple_failed(caplog):
    caplog.set_level(logging.DEBUG)
    resources = Resources(ResourcesType.LOCAL, [Node('n1', 2), Node('n2', 2)])

    stats = { 'executed': 0, 'failed': 0 }
    allocations = list()

    def execute_it(job_iterations, iteration, allocation):
        print(f'executing iteration {job_iterations.id}:{iteration} on allocation {allocation}...')
        stats['executed'] = stats['executed'] + 1
        allocations.append(allocation)

    def failed_it(job_iterations, iteration, message):
        print(f'iteration {job_iterations.id}:{iteration} failed ...')
        stats['failed'] = stats['failed'] + 1

    d1 = JobDescription(name='job1', execution={'exec': 'date'}, resources={'cores': {'exact': 5}})
    j1_total_iterations = 1
    j1 = JobIterations('job1-id', d1, {}, list(range(j1_total_iterations)))

    queue = ScheduleQueue(Scheduler(resources), execute_it, failed_it)
    queue.enqueue([j1])

    queue.schedule_loop()
    assert all((stats['executed'] == 0,
                stats['failed'] == 1,
                len(allocations) == 0))

    queue.schedule_loop()
    assert all((stats['executed'] == 0,
                stats['failed'] == 1,
                len(allocations) == 0))
