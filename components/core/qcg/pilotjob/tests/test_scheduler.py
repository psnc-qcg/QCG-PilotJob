import pytest
#import functools
#import operator

from qcg.pilotjob.resources import CRType, CR, CRBind, Node, ResourcesType, Resources
from qcg.pilotjob.scheduler import Scheduler
from qcg.pilotjob.joblist import JobResources
from qcg.pilotjob.errors import *


def test_scheduler_allocate_cores():
    nCores = [ 8, 8, 8, 8 ]
    r = Resources(ResourcesType.LOCAL, [
            Node("n.{}".format(i), total_cores=nCores[i], used=0) for i in range(len(nCores))
            ])

    s = Scheduler(r)
    assert s

    assert all((len(r.nodes) == len(nCores), [r.nodes[i] == "n.{}".format(i) for i in range(len(nCores))]))
    assert all([r.nodes[i].total == nCores[i] and r.nodes[i].used == 0 and r.nodes[i].free == nCores[i] for i in range(len(nCores))])
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores), r.used_cores == 0))

    c1 = 4
    a1 = s.allocate_cores(c1)
    assert a1 and all((a1.cores == c1, len(a1.nodes) == 1, a1.nodes[0].cores == [str(cid) for cid in range(c1)], a1.nodes[0].crs == None))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c1, r.used_cores == c1))

    c2_min = 4
    c2_max = 8
    a2 = s.allocate_cores(c2_min, c2_max)
    assert a2 and all((a2.cores == c2_max, len(a2.nodes) == 2,
        a2.nodes[0].cores == [str(cid) for cid in range(c1, nCores[0])], a2.nodes[0].crs == None,
        a2.nodes[1].cores == [str(cid) for cid in range(c1 + c2_max - nCores[0])], a2.nodes[0].crs == None))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c1 - c2_max, r.used_cores == c1 + c2_max))

    s.release_allocation(a1)
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c2_max, r.used_cores == c2_max))

    c3_min = 2
    c3_max = 4
    a3 = s.allocate_cores(c3_min, c3_max)
    assert a3 and all((a3.cores == c3_max, len(a3.nodes) == 1,
        a3.nodes[0].cores == [str(cid) for cid in range(c3_max)], a3.nodes[0].crs == None))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c2_max - c3_max, r.used_cores == c2_max + c3_max))

    c4 = 20
    a4 = s.allocate_cores(c4)
    assert a4 and all((a4.cores == c4, len(a4.nodes) == 3,
        a4.nodes[0].cores == [str(cid) for cid in range(c1 + c2_max - nCores[0], nCores[1])], a4.nodes[0].crs == None,
        a4.nodes[1].cores == [str(cid) for cid in range(nCores[2])], a4.nodes[1].crs == None,
        a4.nodes[2].cores == [str(cid) for cid in range(nCores[3])], a4.nodes[2].crs == None))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c2_max - c3_max - c4 == 0, r.used_cores == c2_max + c3_max + c4 == r.total_cores))

    # no more available resources
    c5 = 1
    a5 = s.allocate_cores(c5)
    assert a5 == None
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c2_max - c3_max - c4 == 0, r.used_cores == c2_max + c3_max + c4 == r.total_cores))

    c6_min = 1
    c6_max = 2
    a6 = s.allocate_cores(c6_min, c6_max)
    assert a6 == None
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - c2_max - c3_max - c4 == 0, r.used_cores == c2_max + c3_max + c4 == r.total_cores))

    # release all allocations
    s.release_allocation(a2)
    s.release_allocation(a3)
    s.release_allocation(a4)
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores), r.used_cores == 0))


def test_scheduler_allocate_job_gpus():
    nCores = [ 8, 8, 8, 8 ]
    nGpus = [ 2, 4, 4, 4 ]
    nCrs = [
        { CRType.GPU: CRBind(CRType.GPU, list(range(nGpu))) } for nGpu in nGpus
    ]
    r = Resources(ResourcesType.LOCAL, [
            Node("n.{}".format(i), total_cores=nCores[i], used=0, crs=nCrs[i]) for i in range(len(nCores))
            ])

    s = Scheduler(r)
    assert s

    assert all((len(r.nodes) == len(nCores), [r.nodes[i] == "n.{}".format(i) for i in range(len(nCores))]))
    assert all([r.nodes[i].total == nCores[i] and r.nodes[i].used == 0 and r.nodes[i].free == nCores[i] for i in range(len(nCores))])
    assert all([len(r.nodes[i].crs) == 1 and CRType.GPU in r.nodes[i].crs and r.nodes[i].crs[CRType.GPU].total_count == nGpus[i] and \
            r.nodes[i].crs[CRType.GPU].used == 0 and r.nodes[i].crs[CRType.GPU].available == nGpus[i] for i in range(len(nCores))])
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores), r.used_cores == 0))

    # allocate half of the first node's cpus and one gpu
    job1_r_c = 4
    job1_r_n = 1
    job1_r_g = 1
    job1_r = JobResources(numCores=job1_r_c, numNodes=job1_r_n, nodeCrs={ 'gpu': job1_r_g })
    assert job1_r

    a1 = s.allocate_job(job1_r)
    assert a1
    assert a1 and all((a1.cores == job1_r_c * job1_r_n, len(a1.nodes) == job1_r_n,
        a1.nodes[0].node.name == "n.0", a1.nodes[0].cores == [str(cid) for cid in range(job1_r_c)]))
    assert a1.nodes[0].crs and ((CRType.GPU in a1.nodes[0].crs, a1.nodes[0].crs[CRType.GPU].count == job1_r_g,
        a1.nodes[0].crs[CRType.GPU].instances == list(range(job1_r_g))))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - job1_r_c, r.used_cores == job1_r_c))


    # try to allocate half of the node's cpus but with two gpu's - allocation should not fit to the first node
    job2_r_c = 4
    job2_r_n = 1
    job2_r_g = 2
    job2_r = JobResources(numCores=job2_r_c, numNodes=job2_r_n, nodeCrs={ 'gpu': job2_r_g })
    assert job2_r

    a2 = s.allocate_job(job2_r)
    assert a2
    assert a2 and all((a2.cores == job2_r_c * job2_r_n, len(a2.nodes) == job2_r_n,
        a2.nodes[0].node.name == "n.1", a2.nodes[0].cores == [str(cid) for cid in range(job2_r_c)]))
    assert a2.nodes[0].crs and ((CRType.GPU in a2.nodes[0].crs, a2.nodes[0].crs[CRType.GPU].count == job2_r_g,
        a2.nodes[0].crs[CRType.GPU].instances == list(range(job2_r_g))))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - job1_r_c - job2_r_c, r.used_cores == job1_r_c + job2_r_c))

    # try to allocate node with exceeding gpu amount on node
    job3_r_c = 1
    job3_r_n = 1
    job3_r_g = max(nGpus) + 2
    job3_r = JobResources(numCores=job3_r_c, numNodes=job3_r_n, nodeCrs={ 'gpu': job3_r_g })
    assert job3_r

    with pytest.raises(NotSufficientResources):
        s.allocate_job(job3_r)

    # try to allocate node with exceeding total gpus amount 
    job4_r_c = 1
    job4_r_n = 4
    job4_r_g = max(nGpus)
    job4_r = JobResources(numCores=job4_r_c, numNodes=job4_r_n, nodeCrs={ 'gpu': job4_r_g })
    assert job4_r

    with pytest.raises(NotSufficientResources):
        s.allocate_job(job4_r)
  
    # allocate gpus across many nodes
    job5_r_c = 2
    job5_r_n = 3
    job5_r_g = 2
    job5_r = JobResources(numCores=job5_r_c, numNodes=job5_r_n, nodeCrs={ 'gpu': job5_r_g })
    assert job5_r

    a5 = s.allocate_job(job5_r)
    assert a5 and all((a5.cores == job5_r_c * job5_r_n, len(a5.nodes) == job5_r_n,
        a5.nodes[0].node.name == "n.1", a5.nodes[0].cores == [str(cid) for cid in range(job2_r_c, job2_r_c + job5_r_c)],
        a5.nodes[1].node.name == "n.2", a5.nodes[1].cores == [str(cid) for cid in range(job5_r_c)],
        a5.nodes[2].node.name == "n.3", a5.nodes[2].cores == [str(cid) for cid in range(job5_r_c)])), str(a5)
    assert a5.nodes[0].crs and ((CRType.GPU in a5.nodes[0].crs, a5.nodes[0].crs[CRType.GPU].count == job5_r_g,
        a5.nodes[0].crs[CRType.GPU].instances == list(range(job5_r_g))))
    assert a5.nodes[1].crs and ((CRType.GPU in a5.nodes[1].crs, a5.nodes[1].crs[CRType.GPU].count == job5_r_g,
        a5.nodes[1].crs[CRType.GPU].instances == list(range(job5_r_g))))
    assert a5.nodes[2].crs and ((CRType.GPU in a5.nodes[2].crs, a5.nodes[2].crs[CRType.GPU].count == job5_r_g,
        a5.nodes[2].crs[CRType.GPU].instances == list(range(job5_r_g))))
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - job1_r_c - job2_r_c - job5_r_c * job5_r_n,
        r.used_cores == job1_r_c + job2_r_c + job5_r_c * job5_r_n))

    # allocate cpu's across many nodes
    job6_r_c = 16
    job6_r = JobResources(numCores=job6_r_c)
    assert job6_r

    a6 = s.allocate_job(job6_r)
    assert a6 and all((a6.cores == job6_r_c, len(a6.nodes) == 4,
        a6.nodes[0].node.name == "n.0", a6.nodes[0].cores == [str(cid) for cid in range(job1_r_c, nCores[0])],
        a6.nodes[1].node.name == "n.1", a6.nodes[1].cores == [str(cid) for cid in range(job2_r_c + job5_r_c, nCores[1])],
        a6.nodes[2].node.name == "n.2", a6.nodes[2].cores == [str(cid) for cid in range(job5_r_c, nCores[2])],
        a6.nodes[3].node.name == "n.3", a6.nodes[3].cores == [str(cid) for cid in range(job5_r_c, job5_r_c + 4)])), str(a6)
    assert ([a6.nodes[i].crs is None for i in range(4)])
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores) - job1_r_c - job2_r_c - job5_r_c * job5_r_n - job6_r_c,
        r.used_cores == job1_r_c + job2_r_c + job5_r_c * job5_r_n + job6_r_c))

    # release all allocations
    a1.release()
    a2.release()
    a5.release()
    a6.release()

    assert all((len(r.nodes) == len(nCores), [r.nodes[i] == "n.{}".format(i) for i in range(len(nCores))]))
    assert all([r.nodes[i].total == nCores[i] and r.nodes[i].used == 0 and r.nodes[i].free == nCores[i] for i in range(len(nCores))])
    assert all([len(r.nodes[i].crs) == 1 and CRType.GPU in r.nodes[i].crs and r.nodes[i].crs[CRType.GPU].total_count == nGpus[i] and \
            r.nodes[i].crs[CRType.GPU].used == 0 and r.nodes[i].crs[CRType.GPU].available == nGpus[i] for i in range(len(nCores))])
    assert all((r.total_cores == sum(nCores), r.free_cores == sum(nCores), r.used_cores == 0))

    #print('current resource status: {}'.format(str(r)))

