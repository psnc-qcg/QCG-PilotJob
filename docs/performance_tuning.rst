Performance tuning
==================

Node launcher agents
--------------------

To launch user jobs in Slurm allocation, the QCG PilotJob service is using its
own services that are started on each of the allocation's node. This
sub-service is called node launcher agent. When used on big allocations, that
contains hundred of nodes, the process of starting node launcher agents can
therefore take longer. Also there is a chance that due to some circumstances
(software or hadrware), the process of node launching agent fail. To deal with
such cases, there are command line options to control the process of starting
node launcher agents:

- ``--nl-init-timeout NL_INIT_TIMEOUT`` - the ``NL_INIT_TIMEOUT`` specify
  number of seconds the service should wait for all node launcher agents start
  (``600`` by default),
- ``--nl-ready-treshold NL_READY_TRESHOLD``- the ``NL_READY_TRESHOLD`` value
  (from range 0.0 - 1.0) control the ration of ready node launcher agents when
  process of executing workflow can be started (``1.0`` by default).

After starting of all node launcher agents, the QCG PilotJob service waits up
to ``NL_INIT_TIMEOUT`` until ``NL_READY_TRESHOLD`` * `total number of agents`
report it's successfull start. When it happen, the execution of the workflow
begins, and jobs are submitted only to those nodes where launcher agents
successfully started. All other agents may register after this time enabling
their nodes for exection. When, from some reason the required number of agents
did not register in given interval, the QCG PilotJob service should report the
error and exit without starting workflow execution.


Reserving a core for QCG PJM
----------------------------

We recommend to use ``--system-core`` parameter for workflows that contains many small jobs (HTC) or bigger allocations
(>256 cores). This will reserve a single core in allocation (on the first node of the allocation) for QCG PilogJob
Manager service.
