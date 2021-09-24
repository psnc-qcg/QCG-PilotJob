File based interface
====================

The *File* interface allows a static sequence of commands (called requests) to be read from a file a
nd performed by the system.

File interface usage
--------------------

To use QCG-PilotJob Manager with the *File* interface we should call either the wrapper command:

.. code:: bash

    $ qcg-pm-service

or directly call the Python module:

.. code:: bash

    $ python -m qcg.pilotjob.service

with the ``--file-path FILE_PATH`` parameter, where ``FILE_PATH`` is a path to the requests file.
For example, the command:

.. code:: bash

    $ qcg-pm-service --file-path reqs.json

will run QCG-PilotJob Manager on requests written in ``reqs.json`` file.


Requests file
-------------

The requests file is a JSON format file containing a sequence of commands (requests).
The file must be staged into the working directory of the QCG-PilotJob Manager job and
passed as an argument of this job invocation. The requests are read in an order they are placed in the file.
In the file mode, QCG-PilotJob Manager outputs all responses to the log file.

Commands
--------
The request is a JSON dictionary with the ``request`` key containing a request command.
The additional data format depends on a specific request command. The following commands are currently supported.

``submit``
~~~~~~~~~~

Submit a list of jobs to be processed by the system. The ``jobs`` key must contain a list of formalised
descriptions of jobs.

The Job description is a dictionary with the following keys:

- ``name`` (*required*) ``String`` - job name, must be unique among all other submitted jobs
- ``iteration`` (*optional*) ``Dict`` - defines a loop for iterative jobs, the either *start* (*optional*)
  and *stop* or *values* keys must be defined; the total number of iterations will be *stop - start*
  (the last index of the sub-job will be *stop - 1*) in case of boundary definition or lenght of *values* array
- ``execution`` (*required*) ``Dict`` - execution description with the following keys:

  - ``exec`` (*optional*) ``String`` - executable name (if available in *$PATH*) or absolute path to the executable,
  - ``args`` (*optional*) ``Array of String`` - list of arguments that will be passed to
    the executable,
  - ``script`` (*optional*) ``String`` - commands for *bash* environment, mutually exclusive with ``exec`` and ``args``
  - ``env`` (*optional*) ``Dict (String: String)`` - environment variables that will
    be appended to the execution environment,
  - ``wd`` (*optional*) ``String`` - a working directory, if not defined the
    working directory (current directory) of QCG-PilotJob Manager will be used. If
    the path is not absolute it is relative to the QCG-PilotJob Manager
    working directory. If the directory pointed by the path does not exist, it is created before
    the job starts.
  - ``stdin``, ``stdout``, ``stderr`` (*optional*) ``String`` - path to the
    standard input , standard output and standard error files respectively.
  - ``modules`` (*optional*) ``Array of String`` - the list of environment modules that should be
    loaded before start of the job
  - ``venv`` (*optional*) ``String`` - the path to the virtual environment inside in job should be started
  - ``model`` (*optional*) ``String`` - the model of execution, currently only *threads* explicit model is supported
    which should be used for OpenMP jobs; if not defined - the default,
    dedicated to the multi processes execution model is used

- ``resources`` (*optional*) ``Dict`` - resource requirements, a dictionary with the following keys:

  - ``numCores`` (*optional*) ``Dict`` - number of cores,
  - ``numNodes`` (*optional*) ``Dict``- number of nodes,

    The specification of ``numCores``/``numNodes`` elements may contain the following keys:

    - ``exact`` (*optional*) ``Number`` - the exact number of cores,
    - ``min`` (*optional*) ``Number`` - minimal number of cores,
    - ``max`` (*optional*) ``Number`` - maximal number of cores,
    - ``scheduler`` (*optional*) ``Dict`` - the type of resource iteration scheduler, the key *name* specify type of
      scheduler and currently the *maximum-iters* and *split-into* names are supported, the optional *params*
      dictionary specifies the scheduler parameters (the ``exact`` and ``min`` /  ``max`` are mutually exclusive).


    If ``resources`` is not defined, the ``numCores`` with ``exact`` set to 1 is taken as the default value.

    The ``numCores`` element without ``numNodes`` specifies requested number of cores on any number of nodes.
    The same element used along with the ``numNodes`` determines the number of cores on each requested node.

    The ``scheduler`` optional key defines the iteration resources scheduler. It is futher described in
    section :ref:`iter-res-schedulers`.

- ``dependencies`` (*optional*) ``Dict`` - a dictionary with the following items:

  - ``after`` (*required*) ``Array of String`` - list of names of jobs that must finish before the job can be executed.
    Only when all listed jobs finish (with ``SUCCESS`` status) the current job is taken into consideration by
    the scheduler and can be executed.


The job description may contain variables (except the job name, which cannot contain any variable or
special character) in the format::

    ${ variable-name }

which are replaced with appropriate values by QCG-PilotJob Manager.

The following set of variables is supported during a request validation:

- ``rcnt`` - a request counter that is incremented with every request
  (for iterative sub-jobs the value of this variable is the same)
- ``uniq`` - a unique identifier of each request (each iterative sub-job has its own unique identifier)
- ``sname`` - a local cluster name
- ``date`` - a date when the request was received
- ``time`` - a time when the request was received
- ``dateTime`` - date and time when the request was received
- ``it`` - an index of a current sub-job (only for iterative jobs)
- ``jname`` - a final job name after substitution of all other used variables to their values

The following variables are handled when resources has been already allocated and before the start of  job execution:

- ``root_wd`` - a working directory of QCG-PilotJob Manager, the parent directory for all
  relative job's working directories
- ``ncores`` - a number of allocated cores for the job
- ``nnodes`` - a number of allocated nodes for the job
- ``nlist`` - a list of nodes allocated for the job separated by the comma


The sample submit job request is presented below:

.. code:: json

    {
        "request": "submit",
        "jobs": [
        {
            "name": "msleep2",
            "execution": {
              "exec": "/bin/sleep",
              "args": [
                "5s"
              ],
              "env": {},
              "wd": "sleep.sandbox",
              "stdout": "sleep2.${ncores}.${nnodes}.stdout",
              "stderr": "sleep2.${ncores}.${nnodes}.stderr"
            },
            "resources": {
              "numCores": {
                "exact": 2
              }
            }
       }
       ]
    }

The example response is presented below:

.. code:: json

    {
      "code": 0,
      "message": "1 jobs submitted",
      "data": {
        "submitted": 1,
        "jobs": [
          "msleep2"
        ]
      }
    }

``listJobs``
~~~~~~~~~~~~

Return a list of registered jobs. No additional arguments are needed.
The example list jobs request is presented below:

.. code:: json

    {
        "request": "listJobs"
    }

The example response is presented below:

.. code:: json

    {
      "code": 0,
      "data": {
        "length": 1,
        "jobs": {
          "msleep2": {
            "status": "QUEUED",
            "inQueue": 0
          }
        }
      }
    }

``jobStatus``
~~~~~~~~~~~~~

Report current status of a given jobs. The ``jobNames`` key must contain a list of job names for which status
should be reported. A single job may be in one of the following states:

- ``QUEUED`` - a job was submitted but there are no enough available resources
- ``EXECUTING`` - a job is currently executed
- ``SUCCEED`` - a finished with 0 exit code
- ``FAILED`` - a job could not be started (for example there is no executable) or a job finished with non-zero exit
  code or a requested amount of resources exceeds a total amount of resources,
- ``CANCELED`` - a job has been cancelled either by a user or by a system
- ``OMITTED`` - a job will never be executed due to the dependencies (a job which this job depends
  on failed or was cancelled).

The example job status request is presented below:

.. code:: json

  {
    "request": "jobStatus",
    "jobNames": [ "msleep2" ]
  }

The example response is presented below:

.. code:: json

 {
    "code": 0,
    "data": {
      "jobs": {
        "msleep2": {
          "status": 0,
          "data": {
            "jobName": "msleep2",
            "status": "SUCCEED"
          }
        }
      }
 	}
 }

The ``status`` key at the top, job's level contains numeric code that represents
the operation return code - 0 means success, where other values means problem
with obtaining job's status (e.g. due to the missing job name).

``jobInfo``
~~~~~~~~~~~

Report detailed information about jobs. The ``jobNames`` key must contain a list of job names for
which information should be reported.

The example job status request is presented below:

.. code:: json

  {
    "request": "jobInfo",
    "jobNames": [ "msleep2", "echo" ]
  }

The example response is presented below:

.. code:: json

     {
      "code": 0,
      "data": {
        "jobs": {
          "msleep2": {
            "status": 0,
            "data": {
              "jobName": "msleep2",
              "status": "SUCCEED",
              "runtime": {
                "allocation": "LAPTOP-CNT0BD0F[0:1]",
                "wd": "/sleep.sandbox",
                "rtime": "0:00:02.027212",
                "exit_code": "0"
              },
              "history": "\n2020-06-08 12:56:06.789757: QUEUED\n2020-06-08 12:56:06.789937: SCHEDULED\n2020-06-08 12:56:06.791251: EXECUTING\n2020-06-08 12:56:08.826721: SUCCEED"
            }
          }
        }
      }
    }

``control``
~~~~~~~~~~~

Controls behaviour of QCG-PilotJob Manager. The specific command must be placed in the``command`` key.
Currently the following commands are supported:
- ``finishAfterAllTasksDone``  This command tells QCG-PilotJob Manager to wait until all submitted jobs finish.
  By default, in the file mode, the QCG-PilotJob Manager application  finishes as soon as all requests are
  read from the request file.

The sample control command request is presented below:

.. code:: json

  {
    "request": "control",
    "command": "finishAfterAllTasksDone"
  }

cancelJob
~~~~~~~~~

Cancel a jobs with a list of their names specified in the ``jobNames`` key. Currently this operation is not supported.

removeJob
~~~~~~~~~

Remove a jobs from the registry. The list of names of a jobs to be removed must be placed in the ``jobNames`` key.
This request can be used in case when there is a need to submit another job with the same name - because all the
job names must be unique a new job cannot be submitted with the same name unless the previous one is removed
from the registry.
The example remove job request is presented below:

.. code:: json

    {
      "request": "removeJob",
      "jobNames": [ "msleep2" ]
    }

The example response is presented below:

.. code:: json

    {
      "data": {
        "removed": 1
      },
      "code": 0
    }

resourcesInfo
~~~~~~~~~~~~~

Return current usage of resources. The information about a number of
available and used nodes/cores is reported. No additional arguments are needed.
The example resources info request is presented below:

.. code:: json

    {
      "request": "resourcesInfo"
    }


The example response is presented below:

.. code:: json

    {
      "data": {
        "total_cores": 8,
        "total_nodes": 1,
        "used_cores": 2,
        "free_cores": 6
      },
      "code": 0
    }

finish
~~~~~~

Finish the QCG-PilotJob Manager application immediately. The jobs being currently executed are killed.
No additional arguments are needed.

The example finish command request is presented below:

.. code:: json

    {
      "request": "finish"
    }

