import pytest
import json

from datetime import timedelta

from qcg.pilotjob.resources import CRType
from qcg.pilotjob.joblist import Job, JobIteration, JobDependencies, ResourceSize, JobResources, JobExecution, \
    JobState, JobList
from qcg.pilotjob.errors import IllegalResourceRequirements, IllegalJobDescription, JobAlreadyExist


def test_job_description_simple():

    # missing 'name' element
    with pytest.raises(Exception):
        jobd = '{ }'
        job = Job(**json.loads(jobd))

    # missing 'execution' element
    with pytest.raises(Exception):
        jobd = '{ "name": "job1" }'
        job = Job(**json.loads(jobd))

    # missing 'resources' element
    with pytest.raises(Exception):
        jobd = '{ "name": "job1", "execution": { "exec": "/bin/date" } }'
        job = Job(**json.loads(jobd))

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2  } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with minimal resource requirements"

    jobd = """{ "name": "job1", 
              "execution": { "script": "#!/bin/bash\\n/bin/date\\n" },
              "resources": { "numCores": 2  } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with minimal resource requirements"

    # wrong job name
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": ":job1", 
                  "execution": { "script": "#!/bin/bash\\n/bin/date\\n" },
                  "resources": { "numCores": 2  } }"""
        Job(**json.loads(jobd))

    # no script nor exec
    with pytest.raises(IllegalJobDescription):
        jobd = '{ "name": "job1", "execution": {  }, "resources": { "numCores": 2 } }'
        Job(**json.loads(jobd))

    # both script and exec
    with pytest.raises(IllegalJobDescription):
        jobd = '{ "name": "job1", "execution": { "exec": "/bin/date", "script": "#!/bin/bash\\n/bin/date\\n" }, "resources": { "numCores": 2 } }'
        Job(**json.loads(jobd))

    # both script and arguments
    with pytest.raises(IllegalJobDescription):
        jobd = '{ "name": "job1", "execution": { "script": "#!/bin/bash\\n/bin/date\\n", "args": [ "1", "2" ] }, "resources": { "numCores": 2 } }'
        Job(**json.loads(jobd))

    # both script and env
    with pytest.raises(IllegalJobDescription):
        jobd = '{ "name": "job1", "execution": { "script": "#!/bin/bash\\n/bin/date\\n", "env": { "var1": "1" } }, "resources": { "numCores": 2 } }'
        Job(**json.loads(jobd))

    # modules ok
    jobd = '{ "name": "job1", "execution": { "exec": "/bin/date", "modules": [ "python/3.6" ] }, "resources": { "numCores": 2 } }'
    job = Job(**json.loads(jobd))
    assert job, "Simple job with single module"

    # modules not as list
    jobd = '{ "name": "job1", "execution": { "exec": "/bin/date", "modules": "python/3.6" }, "resources": { "numCores": 2 } }'
    job = Job(**json.loads(jobd))
    assert job, "Simple job with module as string"

    # arguments not as list
    with pytest.raises(IllegalJobDescription):
        jobd = '{ "name": "job1", "execution": { "exec": "/bin/date", "args": "illegal_argument_format" }, "resources": { "numCores": 2 } }'
        Job(**json.loads(jobd))

    # environment not as list
    with pytest.raises(IllegalJobDescription):
        jobd = '{ "name": "job1", "execution": { "exec": "/bin/date", "env": "illegal_environment_list" }, "resources": { "numCores": 2 } }'
        Job(**json.loads(jobd))


def test_job_description_resources():
    # a resource size

    # range
    rs = ResourceSize(min=4, max=5)
    assert all((rs.exact is None, rs.scheduler is None, rs.min==4, rs.max==5, not rs.is_exact()))
    assert rs.range == (4, 5)

    # exact
    rs = ResourceSize(exact=4)
    assert all((rs.exact==4, rs.scheduler is None, rs.min is None, rs.max is None, rs.is_exact()))
    assert rs.range == (None, None)

    # range with scheduler (just for tests0
    rs = ResourceSize(min=4, max=5, scheduler="sched1")
    assert all((rs.exact is None, rs.scheduler=="sched1", rs.min==4, rs.max==5, not rs.is_exact()))
    assert rs.range == (4, 5)

    # exact with scheduler
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(exact=4, scheduler="sched1")

    # no data
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize()

    # no required data
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(scheduler="shed1")

    # range and exact
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(exact=4, min=2)

    # range and exact
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(exact=4, max=2)

    # illegal exact
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(exact=-1)

    # illegal range
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(max=-2)
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(min=-2)
    with pytest.raises(IllegalResourceRequirements):
        ResourceSize(min=4, max=2)

    # serialization with range
    rs = ResourceSize(min=4, max=5, scheduler="sched1")
    assert all((rs.exact is None, rs.scheduler=="sched1", rs.min==4, rs.max==5, not rs.is_exact()))
    assert rs.range == (4, 5)
    rs_json = rs.to_json()

    rs_clone = ResourceSize(**json.loads(rs_json))
    assert all((rs_clone.exact is None, rs_clone.scheduler=="sched1", rs_clone.min==4, rs_clone.max==5, not rs_clone.is_exact()))
    assert rs_clone.range == (4, 5)
    rs_clone.to_dict() == rs_clone.to_dict()

    # serialization with exact
    rs = ResourceSize(exact=2)
    assert all((rs.exact==2, rs.scheduler is None, rs.min is None, rs.max is None, rs.is_exact()))
    assert rs.range == (None, None)
    rs_json = rs.to_json()

    rs_clone = ResourceSize(**json.loads(rs_json))
    assert all((rs_clone.exact==2, rs_clone.scheduler is None, rs_clone.min is None, rs_clone.max is None, rs_clone.is_exact()))
    assert rs_clone.range == (None, None)
    rs_clone.to_dict() == rs_clone.to_dict()

    # number of cores as a number
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2  } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with integer number of cores"

    # number of cores as an exact object
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "exact": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as an exact object"

    # number of cores as a range object
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2, "max": 3 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as a range object"

    # number of cores as a range object, with one of the boundary
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as a range object with only min boundary"

    # number of cores as a range object, with one of the boundary
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "max": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Simple job with number of cores as a range object with only max boundary"

    # empty resources element
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { } }"""
        Job(**json.loads(jobd))

    # no cores specification
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { } } }"""
        Job(**json.loads(jobd))

    # no nodes specification
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numNodes": { } } }"""
        Job(**json.loads(jobd))

    # illegal type of resources specification
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": [ "numNodes" ] }"""
        Job(**json.loads(jobd))

    # illegal type of cores specification
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": [ 1 ] } }"""
        Job(**json.loads(jobd))

    # illegal type of nodes specification
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numNodes": [ 1 ] } }"""
        Job(**json.loads(jobd))

    # exact number with range
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "exact": 2, "min": 1, "max": 3 } } }"""
        job = Job(**json.loads(jobd))

    # exact number with one of the range boundary
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "exact": 2, "min": 1 } } }"""
        job = Job(**json.loads(jobd))

    # number of cores negative
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": -2 } }"""
        job = Job(**json.loads(jobd))

    # 'max' greater than 'min' in range object
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "min": 4, "max": 3 } } }"""
        job = Job(**json.loads(jobd))

    # range boundary negative
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "min": -2 } } }"""
        job = Job(**json.loads(jobd))

    # range boundary negative
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "min": -4, "max": -1 } } }"""
        job = Job(**json.loads(jobd))

    # general crs
    assert not JobResources(numCores=1).has_crs
    jr = JobResources(numCores=1, nodeCrs={ 'gpu': 1 })
    assert all((jr.has_crs, len(jr.crs)==1, CRType.GPU in jr.crs, jr.crs[CRType.GPU] == 1))

    # crs without cores count
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "nodeCrs": { "gpu": 1 } } }"""
        job = Job(**json.loads(jobd))

    # gpu cr
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "gpu": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (gpu)"

    # mem cr
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "mem": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (mem)"

    # gpu cr with non-uniform letter case
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "GpU": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (gpu)"

    # mem cr with non-uniform letter case
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "meM": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (mem)"

    # many crs with non-uniform letter case
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "nodeCrs": { "meM": 1, "gPU": 2 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (mem)"

    # unknown cr
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "strange_cr": 1 } } }"""
        Job(**json.loads(jobd))

    # gpu cr without integer value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": "1" } } }"""
        Job(**json.loads(jobd))

    # mem cr without integer value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "mem": "one" } } }"""
        Job(**json.loads(jobd))

    # gpu cr with negative integer value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": -1 } } }"""
        Job(**json.loads(jobd))

    # gpu cr with 0
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": 0 } } }"""
        Job(**json.loads(jobd))

    # repeating gpu cr
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": 1, "GpU": 2 } } }"""
        Job(**json.loads(jobd))

    # repeating gpu cr
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": { "gpu": 1, "mem": 2, "GpU": 2 } } }"""
        Job(**json.loads(jobd))

    # wrong format of cr's
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2, "nodeCrs": [  ] } }"""
        Job(**json.loads(jobd))

    # num cores & nodes as integers
    jr = JobResources(numCores=2)
    assert all((jr.cores.is_exact(), jr.cores.exact==2))
    jr = JobResources(numNodes=3)
    assert all((jr.nodes.is_exact(), jr.nodes.exact==3))

    # min number of cores
    assert JobResources(numCores=ResourceSize(exact=2)).get_min_num_cores() == 2
    assert JobResources(numCores=ResourceSize(min=4)).get_min_num_cores() == 4
    assert JobResources(numCores=ResourceSize(min=3, max=6)).get_min_num_cores() == 3
    assert JobResources(numCores=ResourceSize(exact=2),
                        numNodes=ResourceSize(exact=1)).get_min_num_cores() == 2
    assert JobResources(numCores=ResourceSize(exact=2),
                        numNodes=ResourceSize(min=4)).get_min_num_cores() == 8
    assert JobResources(numCores=ResourceSize(exact=2),
                        numNodes=ResourceSize(min=4, max=5)).get_min_num_cores() == 8
    assert JobResources(numCores=ResourceSize(min=3, max=6),
                        numNodes=ResourceSize(exact=2)).get_min_num_cores() == 6
    assert JobResources(numCores=ResourceSize(min=3, max=6),
                        numNodes=ResourceSize(min=4, max=6)).get_min_num_cores() == 12

    # walltime in job resources
    jr = JobResources(numCores=ResourceSize(exact=2), wt="10m")
    assert jr.wt == timedelta(minutes=10)

    # errors
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="")
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="0")
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="2")
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="2d")
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="two hours")
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="-10")
    with pytest.raises(IllegalResourceRequirements):
        JobResources(numCores=ResourceSize(exact=2), wt="-10s")

    # job resources serialization
    jr = JobResources(numCores=ResourceSize(min=2, max=6, scheduler="sched1"), numNodes=ResourceSize(exact=4),
                      wt="10m", nodeCrs={ "gpu": 2 })
    jr_json = jr.to_json()

    jr_clone = JobResources(**json.loads(jr_json))
    jr.to_dict() == jr_clone.to_dict()

    # walltime
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "wt": "10m" } }"""
    job = Job(**json.loads(jobd))
    assert job.resources.wt == timedelta(minutes=10)

    # walltime 2
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "wt": "24h" } }"""
    job = Job(**json.loads(jobd))
    assert job.resources.wt == timedelta(hours=24)

    # walltime 3
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "wt": "24h10m5s" } }"""
    job = Job(**json.loads(jobd))
    assert job.resources.wt == timedelta(hours=24, minutes=10, seconds=5)

    # missing walltime value
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "wt": "" } }"""
        job = Job(**json.loads(jobd))
        print('job walltime: {}'.format(str(job.resources.wt)))

    # wrong walltime format walltime
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "wt": "2d" } }"""
        Job(**json.loads(jobd))

    # wrong walltime format walltime 2
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2, "wt": "2" } }"""
        Job(**json.loads(jobd))


def test_job_description_dependencies():
    # simple dependencies
    jobd = """{ "name": "job1", 
              "dependencies": { "after": [ "job2" ] },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert all((len(job.dependencies.after) == 1, 'job2' in job.dependencies.after))

    # valid dependencies
    jobd = """{ "name": "job1", 
              "dependencies": { "after": [ "job2", "job3" ] },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert all((len(job.dependencies.after) == 2, 'job2' in job.dependencies.after, 'job3' in job.dependencies.after))

    jobd = """{ "name": "job1", 
              "dependencies": { "after": [ "job2", "job3" ] },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert job.has_dependencies

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert not job.has_dependencies

    jobd = """{ "name": "job1", 
              "dependencies": { "after": [  ] },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert not job.has_dependencies

    # dependencies not as list
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "dependencies": { "after": "job2" },
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # wrong keyword in job dependencies
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
              "dependencies": { "whenever": "job2" },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # wrong type of job dependencies
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
              "dependencies": { "after": { "job": "job2" } },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # wrong elements of job dependencies
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
              "dependencies": { "after": [ "job2", [ "job3", "job4" ] ] },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # wrong type of job dependencies
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
              "dependencies": [ { "after": [ "job2", [ "job3", "job4" ] ] } ],
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # json serialization
    jdep = JobDependencies(after = [ "job2", "job3"])
    jdep_json = jdep.to_json()
    assert all((jdep.has_dependencies, len(jdep.after) == 2, 'job2' in jdep.after, 'job3' in jdep.after))

    print("job dependencies as json: {}".format(jdep_json))
    jdep_clone = JobDependencies(**json.loads(jdep_json))
    jdep.to_dict() == jdep_clone.to_dict()

    # serialization
    jobd = """{ "name": "job1", 
              "dependencies": { "after": [ "job2", "job3" ] },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with dependencies"
    job_dict = job.to_dict()

    job_clone = json.loads(job.to_json())
    assert job_dict == job_clone


def test_job_description_iterations():
    # no iterations
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert not job.has_iterations

    # valid iterations
    jobd = """{ "name": "job1", 
              "iteration": { "start": 0, "stop": 10 },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert all((job.has_iterations, job.iteration.iterations() == 10))
    assert all(job.iteration.in_range(i) for i in range(10))
    assert all(not job.iteration.in_range(i) for i in range(10, 20))

    # valid iterations with default start
    jobd = """{ "name": "job1", 
              "iteration": { "stop": 10 },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert all((job.has_iterations, job.iteration.iterations() == 10))
    assert all(job.iteration.in_range(i) for i in range(10))
    assert all(not job.iteration.in_range(i) for i in range(10, 20))

    # valid iterations
    jobd = """{ "name": "job1", 
              "iteration": { "start": 5, "stop": 10 },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert all((job.has_iterations, job.iteration.iterations() == 5))
    assert all(job.iteration.in_range(i) for i in range(5, 10))
    assert all(not job.iteration.in_range(i) for i in range(0, 5))

    # not valid iteration type
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "iteration": [ 5, 10 ],
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # not valid iteration spec
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "iteration": {  },
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # not valid iteration spec
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "iteration": { "iterations": 5 },
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # not valid iteration spec
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "iteration": { "start": 5, "stop": 0, "step": 2 },
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # wrong iteration range
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "iteration": { "start": 5, "stop": 0 },
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # wrong iteration range
    with pytest.raises(IllegalJobDescription):
        jobd = """{ "name": "job1", 
                  "iteration": { "start": 5, "stop": 5 },
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": 2 } }"""
        Job(**json.loads(jobd))

    # json serialization
    jit = JobIteration(start=0, stop=10)
    jit_json = jit.to_json()

    jit_clone = JobIteration(**json.loads(jit_json))
    jit.to_dict() == jit_clone.to_dict()

    # string serialization
    assert str(jit) == '{}-{}'.format(0, 10)

    # serialization
    jobd = """{ "name": "job1", 
              "iteration": { "start": 5, "stop": 10 },
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": 2 } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with dependencies"
    job_dict = job.to_dict()

    job_clone = json.loads(job.to_json())
    assert job_dict == job_clone


def test_job_description_resources_schedulers():
    # exact # of cores and scheduler
    with pytest.raises(IllegalResourceRequirements):
        jobd = """{ "name": "job1", 
                  "execution": { "exec": "/bin/date" },
                  "resources": { "numCores": { "exact": 2, "scheduler": { "name": "split-into", "params": { "parts": 8  } } } } }"""
        Job(**json.loads(jobd))

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2, "scheduler": { "name": "split-into", "params": { "parts": 8  } } } } }"""
    job = Job(**json.loads(jobd))
    assert job

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "max": 3, "scheduler": { "name": "split-into", "params": { "parts": 8  } } } } }"""
    job = Job(**json.loads(jobd))
    assert job

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2, "max": 3, "scheduler": { "name": "split-into", "params": { "parts": 8  } } } } }"""
    job = Job(**json.loads(jobd))
    assert job

    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date" },
              "resources": { "numCores": { "min": 2, "scheduler": { "name": "maximum-iters" } } } }"""
    job = Job(**json.loads(jobd))
    assert job


def test_job_description_serialization():
    # gpu cr
    jobd = """{ "name": "job1", 
              "execution": { "exec": "/bin/date", "stdin": "in_file", "stdout": "out_file", "stderr": "err_file",
                "modules": [ "python/3.6" ], "venv": [ "venv-3.6" ] },
              "resources": { "numCores": { "min": 2, "max": 4}, "numNodes": { "min": 1, "max": 2}, "nodeCrs": { "gpu": 1 } } }"""
    job = Job(**json.loads(jobd))
    assert job, "Job with node consumable resources (gpu)"
    job_dict = job.to_dict()

    job_clone = json.loads(job.to_json())
    assert job_dict == job_clone


def test_job_description_attributes():
    # attributes serialization
    attrs = { 'j1_name': 'j1', 'j1_var1': 'var1' }
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            attributes=attrs)
    assert all((len(j.attributes) == len(attrs), j.attributes == attrs))
    j_json = j.to_json()
    j_clone = Job(**json.loads(j_json))
    assert all((len(j_clone.attributes) == len(attrs), j_clone.attributes == attrs))
    assert j.to_dict() == j_clone.to_dict()

    # attributes wrong format
    with pytest.raises(IllegalJobDescription):
        Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            attributes="some_illegal_attributes")
    with pytest.raises(IllegalJobDescription):
        Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            attributes=["some_illegal_attributes", "more_illegal_attributes"])


def test_job_description_subjobs():
    # iteration job
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations

    # iteration names
    assert j.get_name() == 'j1'
    assert all(j.get_name(it) == '{}:{}'.format(j.get_name(), it) for it in range(10))

    # iteration states (initial)
    assert j.state() == JobState.QUEUED
    assert all(j.state(it) == JobState.QUEUED for it in range(10))
    assert all(j.str_state(it) == JobState.QUEUED.name for it in range(10))

    # iteration runtimes
    for it in range(10):
        j.append_runtime({ 'host': 'local.{}'.format(it) }, it)
    assert all(j.runtime(it).get('host') == 'local.{}'.format(it) for it in range(10))

    # whole job success
    for it in range(10):
        j.set_state(JobState.SUCCEED, it, 'job {} succeed'.format(it))
    assert j.state() == JobState.SUCCEED
    assert all(j.messages(it) == 'job {} succeed'.format(it) for it in range(10))

    # whole job fail (one of the iteration failed)
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    for it in range(9):
        j.set_state(JobState.SUCCEED, it, 'job {} succeed'.format(it))
    j.set_state(JobState.FAILED, 9, 'job 9 failed')
    assert j.state() == JobState.FAILED
    assert all(j.messages(it) == 'job {} succeed'.format(it) for it in range(9))
    assert j.messages(9) == 'job 9 failed'

    # whole job fail (one of the iteration canceled)
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    for it in range(9):
        j.set_state(JobState.SUCCEED, it, 'job {} succeed'.format(it))
    j.set_state(JobState.CANCELED, 9, 'job 9 canceled')
    assert j.state() == JobState.FAILED
    assert all(j.messages(it) == 'job {} succeed'.format(it) for it in range(9))
    assert j.messages(9) == 'job 9 canceled'

    # whole job fail (one of the iteration omitted)
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    for it in range(9):
        j.set_state(JobState.SUCCEED, it, 'job {} succeed'.format(it))
    j.set_state(JobState.OMITTED, 9, 'job 9 omitted')
    assert j.state() == JobState.FAILED
    assert all(j.messages(it) == 'job {} succeed'.format(it) for it in range(9))
    assert j.messages(9) == 'job 9 omitted'

    # not all iterations finished
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    for it in range(9):
        j.set_state(JobState.SUCCEED, it, 'job {} succeed'.format(it))
    assert j.state() == JobState.QUEUED
    assert all(j.messages(it) == 'job {} succeed'.format(it) for it in range(9))

    # whole job fail (just one succeed
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    for it in range(9):
        j.set_state(JobState.FAILED, it, 'job {} failed'.format(it))
    j.set_state(JobState.CANCELED, 9, 'job 9 succeed')
    assert j.state() == JobState.FAILED
    assert all(j.messages(it) == 'job {} failed'.format(it) for it in range(9))
    assert j.messages(9) == 'job 9 succeed'

    # many messages per iteration
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    for it in range(10):
        j.set_state(JobState.EXECUTING, it, 'job {} executing'.format(it))
    assert j.state() == JobState.QUEUED
    assert all(j.state(it) == JobState.EXECUTING for it in range(10))
    assert all(j.str_state(it) == JobState.EXECUTING.name for it in range(10))
    for it in range(10):
        j.set_state(JobState.SUCCEED, it, 'job {} finished'.format(it))
    assert j.state() == JobState.SUCCEED
    assert all(j.state(it) == JobState.SUCCEED for it in range(10))
    assert all(j.str_state(it) == JobState.SUCCEED.name for it in range(10))
    assert all(j.messages(it) == 'job {it} executing\njob {it} finished'.format(it=it) for it in range(10))

    # messages for job
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1),
            iteration=JobIteration(stop=10))
    assert j.has_iterations
    j.set_state(JobState.EXECUTING, iteration=None, err_msg='job executing')
    assert all((j.state() == JobState.EXECUTING, j.str_state() == JobState.EXECUTING.name))
    assert j.messages() == 'job executing'
    for it in range(10):
        j.set_state(JobState.EXECUTING, it, 'job {} executing'.format(it))
    j.set_state(JobState.FAILED, iteration=None, err_msg='job failed')
    assert all((j.state() == JobState.FAILED, j.str_state() == JobState.FAILED.name))
    assert j.messages() == 'job executing\njob failed'
    # failed job will not change state if once set
    for it in range(10):
        j.set_state(JobState.SUCCEED, it, 'job {} finished'.format(it))
    assert all((j.state() == JobState.FAILED, j.str_state() == JobState.FAILED.name))
    assert all(j.str_state(it) == JobState.SUCCEED.name for it in range(10))
    assert all(j.messages(it) == 'job {it} executing\njob {it} finished'.format(it=it) for it in range(10))


def test_jobdescription_jobname():
    # job name ok
    j = Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1))
    assert j

    # missing job name
    with pytest.raises(IllegalJobDescription):
        Job(name=None, execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1))

    # illegal character in job name
    with pytest.raises(IllegalJobDescription):
        Job(name='j1:1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1))


def test_joblist():
    jlist = JobList()

    # adding and removing jobs from list
    jlist.add(Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1)))
    assert jlist.exist('j1')
    assert jlist.get('j1').get_name() == 'j1'

    jlist.add(Job(name='j2', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1)))
    assert all((jlist.exist('j1'), jlist.exist('j2')))
    assert jlist.get('j2').get_name() == 'j2'

    with pytest.raises(JobAlreadyExist):
        jlist.add(Job(name='j2', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1)))

    with pytest.raises(JobAlreadyExist):
        jlist.add(Job(name='j1', execution=JobExecution(exec='/bin/date'), resources=JobResources(numCores=1)))

    jnames = jlist.jobs()
    assert all((len(jnames) == 2, 'j1' in jnames, 'j2' in jnames))

    jlist.remove('j2')
    assert not jlist.exist('j2')
    assert jlist.get('j2') is None

    jnames = jlist.jobs()
    assert all((len(jnames) == 1, 'j1' in jnames))

    # try to add something which is not a job
    with pytest.raises(Exception):
        jlist.add('another job')

    # parsing job iteration names
    assert JobList.parse_jobname('j1') == ('j1', None)
    assert JobList.parse_jobname('j1:1') == ('j1', 1)
    with pytest.raises(ValueError):
        assert JobList.parse_jobname('j1:2:1') == ('j1', '2:1')
    with pytest.raises(ValueError):
        assert JobList.parse_jobname('j1:') == ('j1', '')

