import pytest

from qcg.pilotjob.inputqueue.jobdesc import JobDescription
from qcg.pilotjob.errors import IllegalJobDescription


def test_jobdescription():
    j = JobDescription(name='job1', execution={'exec': 'date'})
    assert all((j, j.resources is None, j.dependencies is None, j.iteration is None))
    j.validate()

    j = JobDescription(name='job1', execution={'exec': 'date'}, resources={'cores': {'exact': 1}})
    assert all((j, j.resources is not None, j.dependencies is None, j.iteration is None))
    assert all((j.resources.cores is not None, j.resources.nodes is None, j.resources.wt is None,
                j.resources.crs is None))
    assert all((j.resources.has_cores, not j.resources.has_nodes, not j.resources.has_crs))
    assert all((j.resources.get_min_num_cores() == 1, j.resources.get_min_node_num_cores() == 1))
    assert all((j.resources.cores.exact == 1, j.resources.cores.min is None, j.resources.cores.max is None,
                j.resources.cores.scheduler is None, j.resources.cores.is_exact()))
    j.validate()

    j = JobDescription(name='job1', execution={'exec': 'date'}, resources={'nodes': {'exact': 1}})
    assert all((j, j.resources is not None, j.dependencies is None, j.iteration is None))
    assert all((j.resources.cores is None, j.resources.nodes is not None, j.resources.wt is None,
                j.resources.crs is None))
    assert all((j.resources.has_nodes, not j.resources.has_cores, not j.resources.has_crs))
    assert all((j.resources.get_min_num_cores() == 1, j.resources.get_min_node_num_cores() == 1))
    assert all((j.resources.nodes.exact == 1, j.resources.nodes.min is None, j.resources.nodes.max is None,
                j.resources.nodes.scheduler is None, j.resources.nodes.is_exact()))
    j.validate()

    #TODO more tests


def test_error_jobdescription():
    with pytest.raises(TypeError):
        JobDescription()

    with pytest.raises(TypeError):
        JobDescription(name='j_00')

    with pytest.raises(TypeError):
        JobDescription(execution={'exec': 'date'})

    j = JobDescription(name=None, execution={'exec': 'date'})
    with pytest.raises(IllegalJobDescription):
        j.validate()

    #TODO more tests
