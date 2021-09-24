Examples
========

QCG-PilotJob Manager can be used in two different ways:

- as an service accessible with API
- as a command line utility to execute static, prepared job workflows in a batch mode

The first method allows to dynamically control the jobs execution.

Example API application
-----------------------

Let's write a simple program that will runs 4 instances of simple bash script.

First, we must create an instance of QCG-PilotJob Manager

.. code:: python

    from qcg.pilotjob.api.manager import LocalManager

    manager = LocalManager()

This default instance, when launched outside Slurm scheduling system allocation, will use all local available CPU's.
To check what resources are available for our future jobs, we call a ``resources`` method.

.. code:: bash

    print('available resources: ', manager.resources())

In return we should give something like::

    available resources: {'total_nodes': 1, 'total_cores': 8, 'used_cores': 0, 'free_cores': 8}

where ``total_cores`` and ``free_cores`` depends on number of cores on machine where we are running this example.
So our programs will have access to all ``free_cores``, and QCG-PilotJob manager will make sure that tasks do not
interfere with each other, so the maximum number of simultaneously running job's will be exact ``free_cores``.

To run jobs, we have to create a list of job descriptions and sent it to the QCG-PilotJob manager.

.. code:: bash

    from qcg.pilotjob.api.job import Jobs
    jobs = Jobs().add(script='echo "job ${it} executed at `date` @ `hostname`"', stdout='job.out.${it}', iteration=4)
    job_ids = manager.submit(jobs)
    print('submited jobs: ', str(job_ids))

In this code, we submitted a job with four iterations. The standard output stream should be redirected to file
job.out with iteration index as postfix. As a program to execute in job iteration, we passed the simple *bash* command.
The above code should print a list with just one element: the submitted job identifier. Because we didn't name our
job, the automatically generated name was returned. The job name can passed as keyword argument ``name`` to ``Jobs.add``
method.

.. note::  In the example above we presented the simplified API to submit a job. In case of more complicated
    scenarios we can use the full JSON description to define a submitted job by using ``Jobs.add_std``
    method where all JSON attributes are passed as keyword parameters.
    The full list of accepted parameters can be found in the ``submit`` command documentation described
    in the :ref:`File based interface` document.


Now we can check the status of our submitted job:

.. code:: bash

    job_status = manager.status(job_ids)
    print('job status: ', job_status)

The ``job_status`` should contain dictionary ``jobs`` with our job status information. Because our job was very short,
and should finish immediately, the ``state`` key of ``data`` dictionary of our job's status, should contain value
``SUCCEED``. For longer jobs, we may want to wait until our submitted jobs finish, to do this we use the ``wait4``
*Manager* method:

.. code:: bash

    manager.wait4(job_ids)

Alternatively we can use the ``wait4all`` method, which will wait until all submitted to the QCG-PilotJob Manager jobs
finish:

.. code:: bash

    manager.wait4all()

If we check current directory, we can see that bunch of ``job.out.`` files has been created with a proper content.
If we want to get detailed information about our job, we can use the ``info`` method:

.. code:: bash

    job_info = manager.info(job_ids)
    print('job detailed information: ', job_info)

In return we will get information about iterations (how many finished successfully, how many failed) and when our job
finished.

It is important to call ``finish`` method at the end of our program. This method sent a proper command to QCG-PilotJob
Manager instance, and terminates the background thread in which the instance has been run.

.. code:: bash

    manager.finish()

QCG-PilotJob Manager creates a directory `.qcgpjm-service-` where the following files are stored:

- ``service.log`` - logs of QCG-PilotJob Manager, very useful in case of problems
- ``jobs.report`` - the file containing information about all finished jobs, by default written in text format, but
  there is an option for JSON format which will be easier to parse.

.. seealso::
    The full documentation of the API methods and it's arguments is available in the :ref:`qcg.pilotjob.api package`
    documentation.

Example batch usage
-------------------

The same jobs we can launch using the batch method and prepared input files. In this mode, we have to create JSON file
with all requests we want to sent to QCG-PilotJob Manager. For example, the file contains jobs we submitted in previous
section will look like this:

.. code:: json

    [
      {
        "request": "submit",
        "jobs": [
          {
            "name": "example",
            "iteration": { "stop": 4 },
            "execution": {
              "script": "echo \"job ${it} executed at `date` @ `hostname`\"",
              "stdout": "job.out.${it}"
            }
          }
        ]
      },
      {
        "request": "control",
        "command": "finishAfterAllTasksDone"
      }
    ]

After placing above content in the JSON file, for example ``jobs.json``, we can execute this workflow with:

.. code:: bash

    $ python -m qcg.pilotjob.service --file-path jobs.json

Alternatively, we can use the ``qcg-pm-service`` command alias, that is installed with ``qcg-pilotjob`` Python package.

.. code:: bash

    $ qcg-pm-service --file-path jobs.json

In the input file, we have placed two requests:

- ``submit`` - with job description we want to run
- ``control`` - with ``finishAfterAllTasksDone`` command, which is required to finish QCG-PilotJob Manager (the service
  might listen also on other interfaces, like ZMQ network interface, and must explicitly know when no more requests will
  come and service may be stopped.

The result of executing QCG-PilotJob Manager with presented example file should be the same as using the API - the bunch
of output files should be created, as well as ``.qcgpjm-service-`` directory with additional files.

