Execution environments
======================

In order to give an impression that an individual QCG-PilotJob task is executed directly by the queuing system
a set of environment variables, typically set by the queuing system, is overwritten and passed to the job.
These variables give the application all typical information about a job it can be interested in,
e.g. the amount of assigned resources. In case of parallel application an appropriate machine file
is created with a list of resources for each task. Additionally to unify the execution regardless of the
queuing system a set of variables independent from a queuing system is defined and passed to tasks.

Slurm execution environment
---------------------------

For the SLURM scheduling system, an execution environment for a single job contains the following set of variables:

- ``SLURM_NNODES`` - a number of nodes
- ``SLURM_NODELIST`` - a list of nodes separated by the comma
- ``SLURM_NPROCS`` - a number of cores
- ``SLURM_NTASKS`` - see ``SLURM_NPROCS``
- ``SLURM_JOB_NODELIST`` - see ``SLURM_NODELIST``
- ``SLURM_JOB_NUM_NODES`` - see ``SLURM_NNODES``
- ``SLURM_STEP_NODELIST`` - see ``SLURM_NODELIST``
- ``SLURM_STEP_NUM_NODES`` - see ``SLURM_NNODES``
- ``SLURM_STEP_NUM_TASKS`` - see ``SLURM_NPROCS``
- ``SLURM_NTASKS_PER_NODE`` - a number of cores on every node listed in ``SLURM_NODELIST`` separated by the comma,
- ``SLURM_STEP_TASKS_PER_NODE`` - see ``SLURM_NTASKS_PER_NODE``
- ``SLURM_TASKS_PER_NODE`` - see ``SLURM_NTASKS_PER_NODE``

QCG Execution environment
-------------------------

To unify the execution environment regardless of the queuing system the following variables are set:

- ``QCG_PM_NNODES`` - a number of nodes
- ``QCG_PM_NODELIST``- a list of nodes separated by the comma
- ``QCG_PM_NPROCS`` - a number of cores
- ``QCG_PM_NTASKS`` - see ``QCG_PM_NPROCS``
- ``QCG_PM_STEP_ID`` - a unique identifier of a job (generated by QCG-PilotJob Manager)
- ``QCG_PM_TASKS_PER_NODE`` - a number of cores on every node listed in ``QCG_PM_NODELIST`` separated by the comma
- ``QCG_PM_ZMQ_ADDRESS`` - an address of the network interface of QCG-PilotJob Manager (if enabled)

