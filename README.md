# QCG-PilotJob
The experimental version of QCG-PilotJob service for execution of many computing tasks inside one allocation with a global queue.
=======
# QCG-PilotJob v 0.14.0-alpha


Author: Piotr Kopta <pkopta@man.poznan.pl>, Tomasz Piontek <piontek@man.poznan.pl>, Bartosz Bosak <bbosak@man.poznan.pl>

Copyright (C) 2017-2022 Poznan Supercomputing and Networking Center


## Overview
The QCG-PilotJob system is designed to schedule and execute many small jobs
inside one scheduling system allocation.  Direct submission of a large group of
jobs to a scheduling system can result in long aggregated time to finish as
each single job is scheduled independently and waits in a queue. On the other
hand the submission of a group of jobs can be restricted or even forbidden by
administrative policies defined on clusters.  One can argue that there are
available job array mechanisms in many systems, however the traditional job
array mechanism allows to run only bunch of jobs having the same resource
requirements while jobs being parts of a multiscale simulation by nature vary
in requirements and therefore need more flexible solutions.

The core components of QCG-PilotJob system are the:
- QCG-PilotJob Input Queue service - schedules user's tasks and send them to
  registered executor services in order to launch them
- QCG-PilotJob Executor service - manages execution of user's tasks inside a
  single scheduling system allocation or on locally available CPU's.

The Input Queue listens for user's requests such as submit a new job, ensures
that the dependencies between jobs are fulfilled and send ready to start jobs
to registered executors. The Input Queue service can be started outside the
scheduling system allocation. The only requirement is the service must be available 
for all Executor services (network communication).

The QCG-PilotJob Executor service connects to the Input Queue service and
listens for jobs ready to execute. After receiving such jobs, they are executed
inside scheduling system allocation (currently only Slurm is supported) or on
locally avaiable CPU's. The service schedules all jobs in so that each job runs
on separate processors.

To allow user's to test their scenarios, QCG-PilotJob Manager supports *local*
execution mode, in which all job's are executed on local machine and doesn't
require any scheduling system allocation.

## Current status
The current version of the service QCG-PilotJob with global queue has an
experimental status, which means that it is not production stable and does not
have all the assumed functionality. Currently, the API of the service allows to
submit a job (including iterative one) and to query its status. Executor
services can be started at any time when the InputQueue service is running,
which allows to e.g. create a new allocation, start the Executor service inside
it and connect to the running InputQueue service, thus increasing the speed of
job processing.

The service was tested by running tasks on the Altair cluster (PSNC) of the
following types:
* sequential (single core)
* parallel OMP (threads)

Support for other models, i.e. intelmpi, openmpi and default (for multi-core
tasks) will appear later.

Work is planned to add the functionality known from the stable version of the
QCG-PilotJob service, i.e:
* detailed information about jobs (including information about specific
  iterations)
* cancellation of running jobs
* mechanism of restarting jobs which, because of service interruption, did not
  manage to finish before the service was closed
* collection of performance metrics for running tasks
* implementation of all parallel application models

Also planned are features currently not available in the stable version of the
QCG-PilotJob service, such as:
* real-time job processing monitoring with performance metrics
* support for disabling the Exectur service at any time - jobs processed by
  such a service will be resubmitted to another, fully operational one
* support of connection routing enabling Executor services to run on different
  computing clusters and/or cloud machines

## Installation
The experimental version of QCG-PilotJob service with the global queue can be
installed with pip from the Git repository:

```bash
$ pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git@globalqueue
```

## Example usage

### Introduction
We assume that the QCG-PilotJob software has been installed in an environment
created via `virtualenv`. After initializing such an environment:

```bash 
$ source venv/bin/activate
```

the following executable programs should be available (`venv/bin`):
* `qcg-service` - starts the QCG-PilotJob InputQueue service
* `qcg-executor` - starts the QCG-PilotJob Executor service
* `qcg-client` - client used to communicate with the QCG-PilotJob InputQueue
  service

### Starting the QCG-PilotJob InputQueue service
We can run this service on an access node of the HPC machine or directly in
the created allocation. In this example we will start the service on the access
node. By default, the service will listen on the public address of the access
node, note that not every such address will be available from compute nodes. In
such cases we can use the `-i` parameter and indicate a specific IP address on
which the service should listen for connections from Executor services
and clients (in the case of the Altair cluster it is `172.16.31.250`). When
debugging problems it may be helpful to use the `-d` option, which prints
detailed messages to the screen.

```bash 
$ qcg-service -i 172.16.31.250 -d
```

After starting, the InputQueue service should print the address where it will
listen for calls from Executor services as well as clients, e.g:

```bash
inputqueue listening @ tcp://172.16.31.250:7040
```

At the moment, there is no option yet to switch a running service to the
background, so to follow next steps we have to use another terminal session, or
use the `screen` program which allows to create new terminals in one session.

### Running the QCG-PilotJob Executor service
We run this service inside the queue system allocation (currently only the
Slurm system is supported), i.e. directly on the compute nodes. The Executor
service will connect to the InputQueue service on startup. The address of the
previously started InputQueue service can be passed using the `-i` parameter,
e.g: `-i tcp://172.16.31.250:7040`, or start the Executor service in the same
directory where we started the InputQueue service - the address of the last
started service is written to the `iq.last_service_address` file, which in the
absence of the `-i` argument is loaded by the Executor service by default. In
the following example we will use this function and run both services in the
same directory.  An example running the service on the Altair computing system
might look as follows:

```bash
$ srun -N 1 --ntasks-per-node=48 -p fast --time=60:00 -A project_test --pty bash -c 'venv/bin/qcg-executor -d'
```

Where of course parameters such as `--ntasks-per-node`, `-p`, `A` will be
specific to each computing system. In this case we used the `srun` command with
a `--pty` parameter, which will cause the messages printed by the running
service to go to our console, and the `srun` program terminate when
the Executor service terminates. We can also use `sbatch` instead of `srun` to
run services in batch mode without access to messages.

After successfully running the Executor service and connecting to the
previously run InputQueue service, we should see a message on the screen:

```bash
successfully connected to inputqueue @ tcp://172.16.31.250:7040
```

### Task launching
After launching the Executor service the jobs that we submit to the InputQueue
service will be started immediately, however, nothing stands in the way to
submit jobs before launching the Executor service, in this case they will be
started only after the first Executor connects. In order to submit jobs we can
run the `qcg-client` command from an access node or, for example, from a
compute node. As with the Executor service, we must either specify the address
of the running InputQueue service (argument `-i`), or run the `qcg-client`
command in the same directory from which the `InputQueue` service was run - in
the latter case, the address stored in the `iq.last_service_address` file will
be automatically used by the client.  Using the example job description (saved
in the `example-job.json` file):

```json
{
    "name": "date-example",
    "execution": {
      "exec": "/bin/date",
      "env": {},
      "wd": "date.sandbox",
      "stdout": "date.${ncores}.${it}.stdout",
      "stderr": "date.${ncores}.${it}.stderr"
    },
    "resources": {
      "cores": {
        "exact": 1
      }
    },
    "iteration": {
        "start": 0,
        "stop": 10
    }
}
```
we can run the job (10 iterations of the job) on the compute node using the
`submit` command of the `qcg-client` command, e.g:

```bash
$ venv/bin/qcg-client submit -f example-job.json
connecting to tcp://172.16.31.250:7040 ...
submiting job description: {
  "cmd": "submit_job",
  "jobs": [
    {
      "name": "date",
      "execution": {
        "exec": "/bin/date",
        "env": {},
        "wd": "date.sandbox",
        "stdout": "date.${ncores}.${it}.stdout".
      },
      "resources": {
        "cores": {
          "exact": 1
        }
      },
      "iteration": {
        "start": 0,
        "stop": 10
      }
    }
  ]
}
jobs submitted with response: { 'code': 0, 'message': '1 submitted', 'data': { 'submitted': 1, 'jobs': ['date']}}
```

With the Executor service running, the above job should finish quite quickly.
To check the current processing status of a job, we can use the `status`
command of the `qcg-client` command, specifying the job ID in addition, e.g:

`bash
$ venv/bin/qcg-client status date
connecting to tcp://172.16.31.250:7040 ...
job info response: { 'code': 0, 'message': 'ok', 'data': { 'jobs': { 'date': { 'status': 'ok', 'state': 'SUCCEED' }}}}
```

In the directory where the InputQueue service was running, a `date.sandbox`
directory should be created with a series of `date.1.X.stdout` files where X
should be between 0-9 (inclusive).

Note that at any time we can run an additional Executor service in a new
allocation of the queue system, which will increase the resources available in
the InputQueue service, thus processing the jobs faster.

Currently, automatic detection of the end of the Executor service is not
implemented in the InputQueue service, so terminating the Executor service
while the InputQueue service is running may cause errors when starting tasks.

### Termination of the InputQueue service

Currently, the InputQueue and Executor services must be stopped manually.
