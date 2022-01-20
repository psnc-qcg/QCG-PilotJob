import pytest
import json

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


def test_jobdescription_serialization():
    example_job_file = 'example-job.json'
    with open(example_job_file, 'rt') as example_job_f:
        example_job_str = example_job_f.read()

    assert example_job_str

    print(f'job description read: {json.dumps(json.loads(example_job_str), indent=2)}')
    job = JobDescription(**json.loads(example_job_str))
    assert job

    job_dict = job.to_dict()
    print(f'job description serialized: {job.to_json(indent=2)}')
    assert job_dict
    print(f'job serialized: {job_dict}')

    job_copy = JobDescription(**job_dict)
    print(f'job description deserialized: {job_copy.to_json(indent=2)}')
    assert job.to_dict() == job_copy.to_dict()


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
