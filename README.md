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

Also planned are features currently not available in the stable version of the QCG-PilotJob service, such as:
* real-time job processing monitoring with performance metrics
* support for disabling the Exectur service at any time - jobs processed by such a service will be resubmitted to another, fully operational one
* support of connection routing enabling Executor services to run on different computing clusters and/or cloud machines

## Installation
The experimental version of QCG-PilotJob service with the global queue can be
installed with pip from the Git repository:

```bash
$ pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git@globalqueue
```

## Example usage

