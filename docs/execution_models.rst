Execution models
======================

The QCG-PJM service manages individual cores, so it assigns specific cores to
the tasks. From the performance perspective, binding tasks to the cores is more
efficient as it separates tasks from each other.

.. note::  To support CPU binding, the service must have information about physical
    available cores in the system. This information is provided by *SLURM* in
    a created allocation, but it is not available in case of logical resources, i.e.
    where user defines *virtual* cores and nodes. So currently the binding is
    supported only when the QCG-PJM service is run inside a *SLURM* allocation.

The binding of single core tasks is achieved with:

- Custom Launching Agent, that uses ``taskset`` command,
- SLURM built-in mechanism based on ``--cpu-bind`` option of the ``srun`` command.

Currently, the parallel tasks that require more than one core are launched
only by the ``srun`` or ``mpirun`` commands. The `mask_cpu` flag of the
``srun``'s ```--cpu-bind`` parameter will contain the CPU masks for all
allocated nodes separated with the comma. When ``mpirun`` command is used to
launch parallel task, either the ``--rankfile`` parameter is used for OpenMPI
model or ``I_MPI_PIN_PROCESSOR_LIST`` environment variable for Intel MPI
model.

Additionally, for all tasks launched by the Slurm with binding supported, the
*QCG_PM_CPU_SET* environment variable will be available and set with core
identifiers separated with comma.

To support process affinity for different parallel applications, QCG-PJM supports
different execution models. Currently the following models are available:

- ``default`` - in this model only a single process is launched within the allocation, which is prepared based on task's resource requirements, the allocation description can be found in environment variables, such as:
  - ``SLURM_NODELIST`` - list of allocated nodes separated by comma character
  - ``SLURM_NTASKS`` - total number of cores
  - ``SLURM_TASKS_PER_NODE`` - number of allocated cores on following nodes, where each element can be in a form ``NODE_NAME`` (a single core on a node) or ``NODE_NAME(xNUM_OF_CORES)`` (many cores on a single node)
  - ``QCG_PM_CPU_SET`` - list of core identifiers on following nodes separated by a comma character
- ``threads`` - is designed for running OpenMP tasks on a single node, the process is started with the ``srun`` command with the ``--cpus-per-task`` parameter set according to a number of cores defined in resource requirements
- ``openmpi`` - the processes are started with the ``mpirun`` command with rankfile created based on task's resource requirements
- ``intelmpi`` - the processes are started with the ``mpirun`` command (from the IntelMPI distribution) with defined multiple components, where each component describing execution node, contains ``-host`` element and ``I_MPI_PIN_PROCESSOR_LIST`` arguments set according to the allocated resources
- ``srunmpi`` - the processes are started with the ``srun`` command with ``--cpu-bind`` parameter set according to the allocated resources; this model should be used only on sites that have penMPI/IntelMPI/Slurm environments configured properly.

The ``openmpi`` and ``srunmpi`` provide additional configuration options that that can be defined in the element ``model_opts``. Currently the following options are supported:

- ``mpirun`` (str) - path to the `mpirun` command that should be used to launch applications, if not defined the default command (should be in the ``PATH`` environment variable) is used
- ``mpirun_args`` (list) - additional arguments that should be passed to the `mpirun` command

We recommend usage of ``srunmpi`` model for MPI applications on HPC sites wherever srun is properly configured to
execute MPI codes.

.. note::

    It is important to define for the ``intelmpi``, ``srunmpi`` and ``openmpi`` models
    appropriate IntelMPI/OpenMPI modules in `executable/modules` element of the job description
    or to load them before staring QCG-PJM.

Examples
--------

1) To use different MPI implementation applications in a single workflow we can define appropriate options

.. code:: json

    [
      {
        "request": "submit",
        "jobs": [
          {
            "name": "intelmpi_task",
            "execution": {
              "exec": "/home/user/my_intelmpi_application",
              "model": "intelmpi",
              "model_opts": {
                  "mpirun": "/opt/exp_soft/local/skylake/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/bin/mpirun"
              },
              "modules": [ "impi" ]
            },
            "resources": {
              "numCores": {
                "exact": 8
              }
            }
          },
          {
            "name": "openmpi_task",
            "execution": {
              "exec": "/home/user/my_openmpi_application",
              "model": "openmpi",
              "model_opts": {
                  "mpirun": "/opt/exp_soft/local/skylake/openmpi/4.1.0_gcc620/bin/mpirun",
                  "mpirun_args": [ "--mca", "rmaps_rank_file_physical", "1" ]
              },
              "modules": [ "openmpi/4.1.0_gcc620" ]
            },
            "resources": {
              "numCores": {
                "exact": 8
              }
            }
          }
        ]
      }
    ]

With this input, QCG-PilotJob service will launch task's `intelmpi_task`
application ``/home/user/my_intelmpi_application`` with the mpirun command path
``/opt/exp_soft/local/skylake/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/bin/mpirun``
and additionally it will load `impi` module. The second task's `openmpi_task`
application ``/home/user/my_openmpi_application`` will be launched with the command
``/opt/exp_soft/local/skylake/openmpi/4.1.0_gcc620/bin/mpirun`` with additional
arguments ``--mca rmaps_rank_file_physical 1`` and the module
``openmpi/4.1.0_gcc620`` loaded before the application's start.

The description for the API looks similar:

.. code:: python

  jobs = Jobs()
  jobs.add(name = 'intelmpi_task', exec = '/home/user/my_intelmpi_application', numCores = { 'exact': 4 }, model = 'intelmpi', model_opts = { 'mpirun': '/opt/exp_soft/local/skylake/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/bin/mpirun' }, modules = [ 'impi' ])
  jobs.add(name = 'openmpi_task', exec = '/home/user/my_openmpi_application', numCores = { 'exact': 4 }, model = 'openmpi', model_opts = { 'mpirun': '/opt/exp_soft/local/skylake/openmpi/4.1.0_gcc620/bin/mpirun', 'mpirun_args': ['--mca', 'rmaps_rank_file_physical', '1']}, modules = [ 'openmpi/4.1.0_gcc620' ])

2) Instead of compiled application, it is possible to use Bash script from which the application is called later. It gives us more possibilities to configure the environment for the application. For example using the following input description:

.. code:: json

    [
      {
        "request": "submit",
        "jobs": [
          {
            "name": "openmpi_task",
            "execution": {
              "exec": "bash",
              "args": [ "-l", "./app_script.sh" ],
              "model": "openmpi",
            },
            "resources": {
              "numCores": {
                "exact": 8
              }
            }
          }
        ]
      }
    ]

The script ``app_script.sh`` could look like the following:

.. code:: bash

    #!/bin/bash

    module load openmpi/4.1.0_gcc620
    /home/user/my_openmpi_application

.. warning::

  It is important to remember, that for the parallel task with a model different that default,
  there will be as many instances created of the script as the required number of cores.
  Thus the actions that should be executed only once per all application's processes should be enclosed
  in the following block:

.. code:: bash

  if [ "x$OMPI_COMM_WORLD_RANK" == "x0" ] || [ "x$PMI_RANK" == "x0" ]; then
    # actions in this block will be executed only for rank 0 of OpenMPI/IntelMPI applications
  endif


