Overview
========

The QCG-PilotJob system is designed to schedule and execute many
small jobs inside one scheduling system allocation. Direct submission of
a large group of jobs to a scheduling system can result in long
aggregated time to finish as each single job is scheduled independently
and waits in a queue. On the other hand the submission of a group of
jobs can be restricted or even forbidden by administrative policies
defined on clusters. One can argue that there are available job array
mechanisms in many systems, however the traditional job array mechanism
allows to run only bunch of jobs having the same resource requirements
while jobs being parts of larger workflows by nature vary in
requirements and therefore need more flexible solutions.

The core component of QCG-PilotJob system is QCG-PilotJob Manager.
From the scheduling system perspective, QCG-PilotJob Manager, is seen as
a single job inside a single user allocation. It means that QCG-PilotJob Manager controls an execution
of a complex experiment consisting of many
jobs on resources reserved for the single job allocation. The manager
listens to user's requests and executes commands like submit job, cancel
job and report resources usage. In order to manage the resources and
jobs the system takes into account both resources availability and
mutual dependencies between jobs. Two interfaces are defined to
communicate with the system: file-based (batch mode) and API based. The former
one is dedicated and more convenient for a static scenarios when a
number of jobs is known in advance to the QCG-PilotJob Manager start.
The API based interface is more general and flexible as it allows to
dynamically send new requests and track execution of previously
submitted jobs during the run-time.

To allow user's to test their scenarios, QCG-PilotJob Manager
supports *local* execution mode, in which all job's are executed on
local machine and doesn't require any scheduling system allocation.

Components
----------
QCG-PilotJob consists of three components:

QCG-PilotJob Core
    the essential part of the software, provides all basic mechanism needed to use QCG-PilotJob
QCG-PilotJob Command Line Tools
    a set of command line tools for reporting and analysis of QCG-PilotJob execution
QCG-PilotJob Executor API
    an alternative, simplified API for QCG-PilotJob
