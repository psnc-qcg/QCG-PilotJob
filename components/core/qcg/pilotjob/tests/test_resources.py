import pytest
import pprint

from qcg.pilotjob.resources import Node, ResourcesType, Resources
from qcg.pilotjob.resources import CRType, CR, CRBind


def test_resources_export():
    nodes = [ Node('n1', 10),
              Node('n2', 4, 0, [ "2", "3", "4", "5" ]) ]
    res = Resources(ResourcesType.LOCAL, nodes, False)

    assert all((res.total_nodes == 2, res.total_cores == sum([n.total for n in nodes]), res.used_cores == 0,
                res.free_cores == res.total_cores, res.binding == False, res.rtype == ResourcesType.LOCAL))

    res_copy = Resources.from_dict(res.to_dict())
    assert all((res_copy.total_nodes == res.total_nodes, res_copy.total_cores == res.total_cores,
                res_copy.used_cores == res.used_cores, res_copy.free_cores == res.free_cores,
                res_copy.binding == res.binding, res_copy.binding == res.binding))

    for idx, n in enumerate(nodes):
        assert all((n.ids == res_copy.nodes[idx].ids, n.ids == res.nodes[idx].ids,
                    n.free_ids == res_copy.nodes[idx].free_ids, n.free_ids == res.nodes[idx].free_ids))

    assert all((len(res.to_json()) > 0, res.to_json() == res_copy.to_json()))

    nodes = [ Node('n1', 10, 3),
              Node('n2', 4, 2, [ "2", "3", "4", "5" ]) ]
    res = Resources(ResourcesType.SLURM, nodes, True)

    assert all((res.total_nodes == 2,
                res.total_cores == sum([n.total for n in nodes]),
                res.used_cores == sum([n.used for n in nodes]),
                res.free_cores == sum([n.total for n in nodes]) - sum([n.used for n in nodes]),
                nodes[0].free_ids == [str(cid) for cid in range(3, 10)],
                nodes[1].free_ids == ["4", "5"],
                res.binding == True, res.rtype == ResourcesType.SLURM)), res.to_json()

    res_copy = Resources.from_dict(res.to_dict())
    assert all((res_copy.total_nodes == res.total_nodes, res_copy.total_cores == res.total_cores,
                res_copy.used_cores == res.used_cores, res_copy.free_cores == res.free_cores,
                res_copy.binding == res.binding, res_copy.binding == res.binding))

    for idx, n in enumerate(nodes):
        assert all((n.ids == res_copy.nodes[idx].ids, n.ids == res.nodes[idx].ids,
                    n.free_ids == res_copy.nodes[idx].free_ids, n.free_ids == res.nodes[idx].free_ids))

    assert all((len(res.to_json()) > 0, res.to_json() == res_copy.to_json()))

#    print(res.to_json())
    n1Tot = 8
    n2Tot = 8
    n1GpuTot = 4
    n2GpuTot = 2
    r = Resources(ResourcesType.LOCAL, [
        Node("n1", total_cores=n1Tot, used=0, core_ids=None, crs={CRType.GPU: CRBind(CRType.GPU, list(range(n1GpuTot)))}),
        Node("n2", total_cores=n2Tot, used=0, core_ids=None, crs={CRType.GPU: CRBind(CRType.GPU, list(range(n2GpuTot)))})
        ], binding=False)

    assert all((r != None, r.binding == False, r.rtype == ResourcesType.LOCAL,
        r.total_nodes == 2, r.total_cores == r.free_cores == n1Tot + n2Tot, r.used_cores == 0))

    assert all((r.nodes[0].name == 'n1', r.nodes[0].total == r.nodes[0].free == n1Tot, r.nodes[0].used == 0))
    assert all((r.nodes[1].name == 'n2', r.nodes[1].total == r.nodes[1].free == n2Tot, r.nodes[1].used == 0))

    n1 = r.nodes[0]
    n2 = r.nodes[1]

    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == 0, n1.crs[CRType.GPU].available == n1GpuTot))
    assert all((len(n2.crs) == 1, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == 0, n2.crs[CRType.GPU].available == n2GpuTot))

    r_copy = Resources.from_dict(r.to_dict())
    assert all((r_copy != None, r_copy.binding == False, r_copy.rtype == ResourcesType.LOCAL,
        r_copy.total_nodes == 2, r_copy.total_cores == r_copy.free_cores == n1Tot + n2Tot, r_copy.used_cores == 0))

    assert all((r_copy.nodes[0].name == 'n1', r_copy.nodes[0].total == r_copy.nodes[0].free == n1Tot, r_copy.nodes[0].used == 0))
    assert all((r_copy.nodes[1].name == 'n2', r_copy.nodes[1].total == r_copy.nodes[1].free == n2Tot, r_copy.nodes[1].used == 0))

    n1 = r_copy.nodes[0]
    n2 = r_copy.nodes[1]

    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == 0, n1.crs[CRType.GPU].available == n1GpuTot))
    assert all((len(n2.crs) == 1, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == 0, n2.crs[CRType.GPU].available == n2GpuTot))

    assert all((len(r_copy.to_json()) > 0, r_copy.to_json() == r.to_json()))

    n1Tot = 8
    n2Tot = 8
    n1MemTot = 128
    n2MemTot = 256
    n2GpuTot = 4
    r = Resources(ResourcesType.LOCAL, [
        Node("n1", total_cores=n1Tot, used=0, core_ids=None, crs={CRType.MEM: CR(CRType.MEM, n1MemTot)}),
        Node("n2", total_cores=n2Tot, used=0, core_ids=None, crs={CRType.MEM: CR(CRType.MEM, n2MemTot),
                                                                CRType.GPU: CRBind(CRType.GPU, list(range(n2GpuTot)))})
        ], binding=False)

    assert all((r != None, r.binding == False, r.rtype == ResourcesType.LOCAL,
        r.total_nodes == 2, r.total_cores == r.free_cores == n1Tot + n2Tot, r.used_cores == 0))

    assert all((r.nodes[0].name == 'n1', r.nodes[0].total == r.nodes[0].free == n1Tot, r.nodes[0].used == 0))
    assert all((r.nodes[1].name == 'n2', r.nodes[1].total == r.nodes[1].free == n2Tot, r.nodes[1].used == 0))

    n1 = r.nodes[0]
    n2 = r.nodes[1]

    assert all((len(n1.crs) == 1, CRType.MEM in n1.crs, n1.crs[CRType.MEM].total_count == n1MemTot,
        n1.crs[CRType.MEM].used == 0, n1.crs[CRType.MEM].available == n1MemTot))
    assert all((len(n2.crs) == 2, CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot,
        CRType.GPU in n2.crs, n2.crs[CRType.GPU].available == n2GpuTot))

    r_copy = Resources.from_dict(r.to_dict())
    assert all((r_copy != None, r_copy.binding == False, r_copy.rtype == ResourcesType.LOCAL,
        r_copy.total_nodes == 2, r_copy.total_cores == r_copy.free_cores == n1Tot + n2Tot, r_copy.used_cores == 0))

    assert all((r_copy.nodes[0].name == 'n1', r_copy.nodes[0].total == r_copy.nodes[0].free == n1Tot, r_copy.nodes[0].used == 0))
    assert all((r_copy.nodes[1].name == 'n2', r_copy.nodes[1].total == r_copy.nodes[1].free == n2Tot, r_copy.nodes[1].used == 0))

    n1 = r_copy.nodes[0]
    n2 = r_copy.nodes[1]

    assert all((len(n1.crs) == 1, CRType.MEM in n1.crs, n1.crs[CRType.MEM].total_count == n1MemTot,
        n1.crs[CRType.MEM].used == 0, n1.crs[CRType.MEM].available == n1MemTot))
    assert all((len(n2.crs) == 2, CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot,
        CRType.GPU in n2.crs, n2.crs[CRType.GPU].available == n2GpuTot))

    assert all((len(r_copy.to_json()) > 0, r_copy.to_json() == r.to_json()))


def test_resources_allocate_general():
    n1Tot = 12
    n2Tot = 10
    r = Resources(ResourcesType.LOCAL, [
            Node("n1", total_cores=n1Tot, used=0, core_ids=None, crs=None),
            Node("n2", total_cores=n2Tot, used=0, core_ids=None, crs=None) ], binding=False)

    assert all((r != None, r.binding == False, r.rtype == ResourcesType.LOCAL,
        r.total_nodes == 2,
        r.total_cores == r.free_cores == n1Tot + n2Tot, r.used_cores == 0))

    assert all((r.nodes[0].name == 'n1', r.nodes[0].total == r.nodes[0].free == n1Tot, r.nodes[0].used == 0))
    assert all((r.nodes[1].name == 'n2', r.nodes[1].total == r.nodes[1].free == n2Tot, r.nodes[1].used == 0))

    # create partial allocation on the first node
    n1 = r.nodes[0]
    c1 = 4
    a1 = n1.allocate_max(c1)
    assert all((a1, a1.ncores == c1, a1.cores == [str(cid) for cid in range(c1)], a1.crs == None))
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1, n1.used == c1))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1, r.used_cores == c1))

    # create partial allocation on the second node
    n2 = r.nodes[1]
    c2 = 8
    a2 = n2.allocate_max(c2)
    assert all((a2, a2.ncores == c2, a2.cores == [str(cid) for cid in range(c2)], a2.crs == None))
    assert all((n2.total == n2Tot, n2.free == n2Tot - c2, n2.used == c2))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1 - c2, r.used_cores == c1 + c2))

    # request for the more resources then are available
    c3 = n1Tot - c1 + 2
    c3Real = n1Tot - c1
    a3 = n1.allocate_max(c3)
    assert all((a3, a3.ncores == c3Real, a3.cores == [str(cid) for cid in range(c1, c1 + c3Real)], a3.crs == None))
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1 - c3Real == 0, n1.used == c1 + c3Real == n1Tot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1 - c2 - c3Real, r.used_cores == c1 + c2 + c3Real))

    # request for no more resources
    c4 = 4
    a4 = n1.allocate_max(c4)
    assert a4 == None
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1 - c3Real == 0, n1.used == c1 + c3Real == n1Tot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1 - c2 - c3Real, r.used_cores == c1 + c2 + c3Real))

    # release the first allocation (now we should have only c3Real allocated cores)
    a1.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot - c3Real, n1.used == c3Real))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c2 - c3Real, r.used_cores == c2 + c3Real))

    # allocate rest of the free cores
    c5 = n1.free
    a5 = n1.allocate_max(c5)
    assert all((a5, a5.ncores == c5, a5.cores == [str(cid) for cid in list(range(c1)) + list(range(c1 + c3Real, n1Tot))], a5.crs == None))
    assert all((n1.total == n1Tot, n1.free == n1Tot - c3Real - c5 == 0, n1.used == c5 + c3Real == n1Tot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c5 - c2 - c3Real, r.used_cores == c5 + c2 + c3Real))

    # release once more the first, already released allocation - nothing should change
    a1.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot - c3Real - c5 == 0, n1.used == c5 + c3Real == n1Tot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c5 - c2 - c3Real, r.used_cores == c5 + c2 + c3Real))

    # release all allocations
    for a in [ a2, a3, a5 ]:
        a.release()

    assert all((r.nodes[0].name == 'n1', r.nodes[0].total == r.nodes[0].free == n1Tot, r.nodes[0].used == 0))
    assert all((r.nodes[1].name == 'n2', r.nodes[1].total == r.nodes[1].free == n2Tot, r.nodes[1].used == 0))
    assert all((r.total_nodes == 2, r.total_cores == r.free_cores == n1Tot + n2Tot, r.used_cores == 0))


def test_resources_allocate_crs_gpu():
    n1Tot = 8
    n2Tot = 8
    n1GpuTot = 4
    n2GpuTot = 2
    r = Resources(ResourcesType.LOCAL, [
        Node("n1", total_cores=n1Tot, used=0, core_ids=None, crs={CRType.GPU: CRBind(CRType.GPU, list(range(n1GpuTot)))}),
        Node("n2", total_cores=n2Tot, used=0, core_ids=None, crs={CRType.GPU: CRBind(CRType.GPU, list(range(n2GpuTot)))})
        ], binding=False)

    assert all((r != None, r.binding == False, r.rtype == ResourcesType.LOCAL,
        r.total_nodes == 2, r.total_cores == r.free_cores == n1Tot + n2Tot, r.used_cores == 0))

    assert all((r.nodes[0].name == 'n1', r.nodes[0].total == r.nodes[0].free == n1Tot, r.nodes[0].used == 0))
    assert all((r.nodes[1].name == 'n2', r.nodes[1].total == r.nodes[1].free == n2Tot, r.nodes[1].used == 0))

    n1 = r.nodes[0]
    n2 = r.nodes[1]

    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == 0, n1.crs[CRType.GPU].available == n1GpuTot))
    assert all((len(n2.crs) == 1, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == 0, n2.crs[CRType.GPU].available == n2GpuTot))

    # create partial allocation on the first node with gpu cr
    c1_c = 2
    c1_g = 2
    a1 = n1.allocate_max(c1_c, {CRType.GPU: c1_g})
    assert a1
    assert all((a1.ncores == c1_c, a1.cores == [str(cid) for cid in range(c1_c)])), "cores: {}".format(str(a1.cores))
    assert a1.crs != None and all((len(a1.crs) == 1, CRType.GPU in a1.crs, a1.crs[CRType.GPU].count == c1_g,
        a1.crs[CRType.GPU].instances == list(range(c1_g)))), "crs: {}".format(str(a1.crs))
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c, n1.used == c1_c))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g, n1.crs[CRType.GPU].available == n1GpuTot - c1_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c, r.used_cores == c1_c))


    # try to allocate more crs than available, the allocation should not be created and state of resources should not change
    c2_c = 2
    c2_g = n1GpuTot - c1_g + 2
    a2 = n1.allocate_max(c2_c, {CRType.GPU: c2_g})
    assert a2 == None
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c, n1.used == c1_c))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g, n1.crs[CRType.GPU].available == n1GpuTot - c1_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c, r.used_cores == c1_c))

    # create allocation for the rest of the cpus at the first node
    c3_c = n1Tot - c1_c
    a3 = n1.allocate_max(c3_c)
    assert a3
    assert all((a3.ncores == c3_c, a3.cores == [str(cid) for cid in range(c1_c, c1_c + c3_c)])), "cores: {} vs expected {}".format(str(a3.cores), str(list(range(c1_c, c1_c + c3_c))))
    assert a3.crs == None
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c - c3_c == 0, n1.used == c1_c + c3_c == n1Tot))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g, n1.crs[CRType.GPU].available == n1GpuTot - c1_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c - c3_c, r.used_cores == c1_c + c3_c))

    # try to allocate available crs but without available cpu's, the allocation should not be created and state of resources should not change
    c4_c = 1
    c4_g = n1GpuTot - c1_g
    a4 = n1.allocate_max(c4_c, {CRType.GPU: c4_g})
    assert a4 == None
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c - c3_c == 0, n1.used == c1_c + c3_c == n1Tot))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g, n1.crs[CRType.GPU].available == n1GpuTot - c1_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c - c3_c, r.used_cores == c1_c + c3_c))

    # release some cpus
    a3.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c, n1.used == c1_c))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g, n1.crs[CRType.GPU].available == n1GpuTot - c1_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c, r.used_cores == c1_c))

    # release already released cpu's - nothing should change
    a3.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c, n1.used == c1_c))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g, n1.crs[CRType.GPU].available == n1GpuTot - c1_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c, r.used_cores == c1_c))

    # allocate rest of the resources
    c5_c = n1.free
    c5_g = n1.crs[CRType.GPU].available
    a5 = n1.allocate_max(c5_c, {CRType.GPU: c5_g})
    assert a5
    assert all((a5.ncores == c5_c, a5.cores == [str(cid) for cid in range(c1_c, c1_c + c5_c)])), "cores: {} vs expected {}".format(str(a5.cores), str(list(range(c1_c, c1_c + c5_c))))
    assert a5.crs != None and all((len(a5.crs) == 1, CRType.GPU in a5.crs, a5.crs[CRType.GPU].count == c5_g,
        a5.crs[CRType.GPU].instances == list(range(c1_g, c1_g + c5_g)))), "crs: {}".format(str(a5.crs))
    assert all((n1.total == n1Tot, n1.free == n1Tot - c1_c - c5_c == 0, n1.used == c1_c + c5_c == n1Tot))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c1_g + c5_g == n1.crs[CRType.GPU].total_count,
        n1.crs[CRType.GPU].available == n1GpuTot - c1_g - c5_g == 0))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c - c5_c, r.used_cores == c1_c + c5_c))

    # release one gpu allocation
    a1.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot - c5_c, n1.used == c5_c))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c5_g, n1.crs[CRType.GPU].available == n1GpuTot - c5_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c5_c, r.used_cores == c5_c))

    # release once more already released gpu allocation - nothing should change
    a1.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot - c5_c, n1.used == c5_c))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == c5_g, n1.crs[CRType.GPU].available == n1GpuTot - c5_g))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c5_c, r.used_cores == c5_c))

    # release rest of the resources
    a5.release()
    a5.release()
    assert all((n1.total == n1Tot, n1.free == n1Tot, n1.used == 0))
    assert all((len(n1.crs) == 1, CRType.GPU in n1.crs, n1.crs[CRType.GPU].total_count == n1GpuTot,
        n1.crs[CRType.GPU].used == 0, n1.crs[CRType.GPU].available == n1GpuTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot, r.used_cores == 0))


def test_resources_allocate_crs_mem():
    n1Tot = 8
    n2Tot = 8
    n1MemTot = 128
    n2MemTot = 256
    n2GpuTot = 4
    r = Resources(ResourcesType.LOCAL, [
        Node("n1", total_cores=n1Tot, used=0, core_ids=None, crs={CRType.MEM: CR(CRType.MEM, n1MemTot)}),
        Node("n2", total_cores=n2Tot, used=0, core_ids=None, crs={CRType.MEM: CR(CRType.MEM, n2MemTot),
                                                                CRType.GPU: CRBind(CRType.GPU, list(range(n2GpuTot)))})
        ], binding=False)

    assert all((r != None, r.binding == False, r.rtype == ResourcesType.LOCAL,
        r.total_nodes == 2, r.total_cores == r.free_cores == n1Tot + n2Tot, r.used_cores == 0))

    assert all((r.nodes[0].name == 'n1', r.nodes[0].total == r.nodes[0].free == n1Tot, r.nodes[0].used == 0))
    assert all((r.nodes[1].name == 'n2', r.nodes[1].total == r.nodes[1].free == n2Tot, r.nodes[1].used == 0))

    n1 = r.nodes[0]
    n2 = r.nodes[1]

    assert all((len(n1.crs) == 1, CRType.MEM in n1.crs, n1.crs[CRType.MEM].total_count == n1MemTot,
        n1.crs[CRType.MEM].used == 0, n1.crs[CRType.MEM].available == n1MemTot))
    assert all((len(n2.crs) == 2, CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot,
        CRType.GPU in n2.crs, n2.crs[CRType.GPU].available == n2GpuTot))


    # create allocation with both CR's
    c1_c = n2.free - 2
    c1_g = n2.crs[CRType.GPU].available - 1
    c1_m = n2.crs[CRType.MEM].available - 20
    a1 = n2.allocate_max(c1_c, {CRType.GPU: c1_g, CRType.MEM: c1_m})
    assert a1
    assert all((a1.ncores == c1_c, a1.cores == [str(cid) for cid in range(c1_c)])), "cores: {} vs expected {}".format(str(a1.cores), str(list(range(c1_c))))
    assert a1.crs != None and all((len(a1.crs) == 2, CRType.GPU in a1.crs, a1.crs[CRType.GPU].count == c1_g,
        a1.crs[CRType.GPU].instances == list(range(c1_g)), CRType.MEM in a1.crs, a1.crs[CRType.MEM].count == c1_m)), "crs: {}".format(str(a1.crs))
    assert all((n2.total == n2Tot, n2.free == n2Tot - c1_c, n2.used == c1_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c1_g, n2.crs[CRType.GPU].available == n2GpuTot - c1_g,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == c1_m, n2.crs[CRType.MEM].available == n2MemTot - c1_m))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c, r.used_cores == c1_c))

    # allocate the rest of the GPU's
    c2_c = n2.free
    c2_g = n2.crs[CRType.GPU].available
    a2 = n2.allocate_max(c2_c, {CRType.GPU: c2_g})
    assert a2
    assert all((a2.ncores == c2_c, a2.cores == [str(cid) for cid in range(c1_c, c1_c + c2_c)])), "cores: {} vs expected {}".format(str(a2.cores), str(list(range(c1_c, c1_c + c2_c))))
    assert a2.crs != None and all((len(a2.crs) == 1, CRType.GPU in a2.crs, a2.crs[CRType.GPU].count == c2_g,
        a2.crs[CRType.GPU].instances == list(range(c1_g, c1_g + c2_g))))
    assert all((n2.total == n2Tot, n2.free == n2Tot - c1_c - c2_c, n2.used == c1_c + c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c1_g + c2_g == n2GpuTot, n2.crs[CRType.GPU].available == n2GpuTot - c1_g - c2_g == 0,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == c1_m, n2.crs[CRType.MEM].available == n2MemTot - c1_m))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c - c2_c, r.used_cores == c1_c + c2_c))

    # try to allocate mem - with no available cpu's
    c3_c = 1
    c3_m = n2.crs[CRType.MEM].available
    a3 = n2.allocate_max(c3_c, {CRType.MEM: c3_m})
    assert a3 == None
    assert all((n2.total == n2Tot, n2.free == n2Tot - c1_c - c2_c, n2.used == c1_c + c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c1_g + c2_g == n2GpuTot, n2.crs[CRType.GPU].available == n2GpuTot - c1_g - c2_g == 0,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == c1_m, n2.crs[CRType.MEM].available == n2MemTot - c1_m))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c - c2_c, r.used_cores == c1_c + c2_c))

    # try to allocate mem - with no available gpu's
    c3_c = 1
    c3_m = n2.crs[CRType.MEM].available
    a3 = n2.allocate_max(c3_c, {CRType.MEM: c3_m})
    assert a3 == None
    assert all((n2.total == n2Tot, n2.free == n2Tot - c1_c - c2_c, n2.used == c1_c + c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c1_g + c2_g == n2GpuTot, n2.crs[CRType.GPU].available == n2GpuTot - c1_g - c2_g == 0,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == c1_m, n2.crs[CRType.MEM].available == n2MemTot - c1_m))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c1_c - c2_c, r.used_cores == c1_c + c2_c))

    # release some resources
    a1.release()
    assert all((n2.total == n2Tot, n2.free == n2Tot - c2_c, n2.used == c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c2_g, n2.crs[CRType.GPU].available == n2GpuTot - c2_g,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c2_c, r.used_cores == c2_c))

    # once more release already released resources - nothing should change
    a1.release()
    assert all((n2.total == n2Tot, n2.free == n2Tot - c2_c, n2.used == c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c2_g, n2.crs[CRType.GPU].available == n2GpuTot - c2_g,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c2_c, r.used_cores == c2_c))

    # allocate rest of cr
    c4_c = n2.free
    c4_g = n2.crs[CRType.GPU].available
    c4_m = n2.crs[CRType.MEM].available
    a4 = n2.allocate_max(c4_c, {CRType.MEM: c4_m,
                               CRType.GPU: c4_g})
    assert a4
    assert all((a4.ncores == c4_c, a4.cores == [str(cid) for cid in range(c1_c)])), "cores: {} vs expected {}".format(str(a4.cores), str(list(range(c1_c))))
    assert a4.crs != None and all((len(a4.crs) == 2, CRType.GPU in a4.crs, a4.crs[CRType.GPU].count == c4_g,
        a4.crs[CRType.GPU].instances == list(range(c4_g)), CRType.MEM in a4.crs, a4.crs[CRType.MEM].count == c4_m)), "crs: {}".format(str(a4.crs))
    assert all((n2.total == n2Tot, n2.free == n2Tot - c2_c - c4_c == 0, n2.used == c2_c + c4_c == n2Tot))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c2_g + c4_g == n2GpuTot, n2.crs[CRType.GPU].available == n2GpuTot - c2_g - c4_g == 0,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot))
    assert all((n2.crs[CRType.MEM].used == c4_m == n2MemTot, n2.crs[CRType.MEM].available == n2MemTot - c4_m == 0))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c2_c - c4_c, r.used_cores == c2_c + c4_c))

    # release last allocation
    a4.release()
    assert all((n2.total == n2Tot, n2.free == n2Tot - c2_c, n2.used == c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c2_g, n2.crs[CRType.GPU].available == n2GpuTot - c2_g,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c2_c, r.used_cores == c2_c))

    # release once more already released resources - nothing should change
    a4.release()
    assert all((n2.total == n2Tot, n2.free == n2Tot - c2_c, n2.used == c2_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c2_g, n2.crs[CRType.GPU].available == n2GpuTot - c2_g,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c2_c, r.used_cores == c2_c))

    # release remaining allocation - all resources should be free
    a2.release()
    assert all((n2.total == n2Tot, n2.free == n2Tot, n2.used == 0))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == 0, n2.crs[CRType.GPU].available == n2GpuTot,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot, r.used_cores == 0))

    # release some of mem cr and whole gpu set
    c5_c = n2.free - 2
    c5_g = n2.crs[CRType.GPU].available
    c5_m = n2.crs[CRType.MEM].available - 20
    a5 = n2.allocate_max(c5_c, {CRType.MEM: c5_m,
                               CRType.GPU: c5_g})
    assert a5
    assert all((a5.ncores == c5_c, a5.cores == [str(cid) for cid in range(c5_c)])), "cores: {} vs expected {}".format(str(a5.cores), str(list(range(c5_c))))
    assert a5.crs != None and all((len(a5.crs) == 2, CRType.GPU in a5.crs, a5.crs[CRType.GPU].count == c5_g,
        a5.crs[CRType.GPU].instances == list(range(c5_g)), CRType.MEM in a5.crs, a5.crs[CRType.MEM].count == c5_m)), "crs: {}".format(str(a5.crs))
    assert all((n2.total == n2Tot, n2.free == n2Tot - c5_c, n2.used == c5_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c5_g == n2GpuTot, n2.crs[CRType.GPU].available == n2GpuTot - c5_g == 0,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot))
    assert all((n2.crs[CRType.MEM].used == c5_m, n2.crs[CRType.MEM].available == n2MemTot - c5_m))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c5_c, r.used_cores == c5_c))

    # try to allocate rest of mem cr and one of the gpu - allocation should not be created
    c6_c = n2.free
    c6_g = 1
    c6_m = n2.crs[CRType.MEM].available
    a6 = n2.allocate_max(c6_c, {CRType.MEM: c6_m,
                               CRType.GPU: c6_g})
    assert a6 == None
    assert all((n2.total == n2Tot, n2.free == n2Tot - c5_c, n2.used == c5_c))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == c5_g == n2GpuTot, n2.crs[CRType.GPU].available == n2GpuTot - c5_g == 0,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot))
    assert all((n2.crs[CRType.MEM].used == c5_m, n2.crs[CRType.MEM].available == n2MemTot - c5_m))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot - c5_c, r.used_cores == c5_c))

    # release all allocations
    a5.release()
    assert all((n2.total == n2Tot, n2.free == n2Tot, n2.used == 0))
    assert all((len(n2.crs) == 2, CRType.GPU in n2.crs, n2.crs[CRType.GPU].total_count == n2GpuTot,
        n2.crs[CRType.GPU].used == 0, n2.crs[CRType.GPU].available == n2GpuTot,
        CRType.MEM in n2.crs, n2.crs[CRType.MEM].total_count == n2MemTot,
        n2.crs[CRType.MEM].used == 0, n2.crs[CRType.MEM].available == n2MemTot))
    assert all((r.total_cores == n1Tot + n2Tot, r.free_cores == n1Tot + n2Tot, r.used_cores == 0))


