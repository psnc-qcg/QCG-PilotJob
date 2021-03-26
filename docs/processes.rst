Processes statistics
====================

QCG-PilotJob Manager support gathering metrics about launched processes. This feature enables analysis of application
behavior such as:

- process tree inspection, from the lauching by QCG-PilotJob to the final application process (with all intermediate steps,
  such as ``srun``, ``mpirun``, ``orted`` etc. with time line (delays between following processes in tree)
- process localization, such as: node name and cpu affinity
- coarse process metrics: cpu and memory utilization


.. note::
    Please note that this is initial version of gathering process metrics, and due to the implementation obtained data might be precise.


How it works
------------

When ``--enable-proc-stats`` has been used with QCG-PilotJob (either as a command line argument or as argument to the ``server_args`` parameter of ``LocalManager`` class), the launcher agent started on each of the Slurm allocation's node stars thread that periodically query about processes started in the local system. Because collecting statistics about all processes in the system would take too much time, and thus reduce the frequency of queries, launcher agent only checks the descendants of the ``slurmstepd`` process. This process is responsible for starting every user process in Slurm, including launcher agent. Therefore we register all process started by launcher agent, and also processes started by MPI that is configured with Slurm (in such situation, Slurm asks ``slurmstepd`` daemon to launch instancesof MPI application). Every descendant process of the ``slurmstepd`` is registered with it's identifier (``pid``) and basic statistics, such as:

- ``pid`` - process identifier
- process name (in most cases name of the executable file)
- command line arguments
- parent process name
- parent process identifier
- cpu affinity - list of available cores
- accumulated process times in seconds (the detailed description of the format is available at https://psutil.readthedocs.io/en/latest/index.html?highlight=cpu_times#psutil.cpu_times)
- memory information (the detailed description of the format is available at https://psutil.readthedocs.io/en/latest/index.html?highlight=cpu_times#psutil.Process.memory_info)

Currently the data from each query is stored in in-memory database and saved to the file at launcher agent finish. The destination file, created in QCG-PilotJob working directory, will have name of following pattern: ``ptrace_{node_name}_{current_date}_{random_number}.log``. In the future releases we are planing to send those information to external service that will allow to run-time monitoring of gathered statistics.

It is worth to mention about some shortcoming of such approach:

- because the processes are queried with some frequency (currently every 1 second), there is a chance that very short living process will not be registered,
- there is a possibility that after finish of some process, another one with the same identifier will be created later


How to use
----------

First of all the ``--enable-proc-stats`` arument of the QCG-PilotJob service must be used either as command line argument, or as one of the ``server_args`` element in ``LocalManager`` class. When all QCG-PilotJob workflow finish, the working directory should contain one or many (for each of allocation nodes there should be instance of this file) files named ``ptrace_{node_name}_{current_date}_{random_number}.log``. Those files contains information about processes statistics in JSON format. To analyze this data, QCG-PilotJob provides ``qcg-pm-processes`` command line tool. Documentation about this tool is available with:

.. code:: bash
    $ qcg-pm-processes --help

 Currently there are two subcommands:

 - ``tree`` - print job process tree
 - ``apps`` - print job processes details

 To use the ``qcg-pm-process`` tool we need to pass two arguments:
 
 - ``WDIR`` - path to the QCG-PilotJob working directory
 - ``JOBIDS`` - one or many job identifiers, **for iterative job's, we need to use the specific iterations, for example: job:0**

 The ``qcg-pm-processes`` tool analyze the processes metrics and tries to correlate processes launched on different nodes as descendants of a single job (the MPI applications).


 Examples
 --------

 Suppose we have submited a following job to the QCG-Pilot:

 .. code:: json

	[{
		"request": "submit",
		"jobs": [{
			"name": "mpi_iter",
			"iteration": { "stop": 6 },
			"execution": {
				"exec": "/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app",
				"stdout": "${it}.stdout",
				"stderr": "${it}.stderr",
				"model": "openmpi"
			},
			"resources": {
				"numCores": {"exact": 8}
			}
		} ]
	}]

In this example we submitted 6 instances of ``mpi_iter`` job, where each instance is an MPI application started on 8 cores.

To get process tree of the first instance of this job:

.. code:: bash

	$ qcg-pm-processes tree out-api-mpi-iter mpi_iter:0
	job mpi_iter:0, job process id 28521, application name openmpi_3.1_gcc_6.2_app
	  --28521:bash (bash -c source /etc/profile; module purge; module load openmpi/3.1.4_gcc620; exe) node(e0025) created 2021-03-25 17:18:34.350000
		--29537:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.83 secs
		--29542:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.86 secs
		--29638:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 4.01 secs
		--29608:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.98 secs
		--29547:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.88 secs
		--29579:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.95 secs
		--29554:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.91 secs
		--29567:openmpi_3.1_gcc_6.2_app (/tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app) node(e0025) after 3.94 secs

To get detail process info:

.. code:: bash

	$ qcg-pm-processes apps out-api-mpi-iter mpi_iter:0
	found 8 target processes
	29537:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.180000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [0]
			cpu times: [0.04, 0.03, 0.0, 0.0, 0.0]
			cpu memory info: [25219072, 525488128, 12701696, 8192, 0, 153374720, 0]
			cpu memory percent: 0.018710530527870046
	29542:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.210000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [1]
			cpu times: [0.06, 0.03, 0.0, 0.0, 0.0]
			cpu memory info: [25206784, 391258112, 12693504, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01870141381655226
	29638:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.360000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [7]
			cpu times: [0.05, 0.03, 0.0, 0.0, 0.0]
			cpu memory info: [25202688, 391258112, 12689408, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01869837491277966
	29608:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.330000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [6]
			cpu times: [0.04, 0.04, 0.0, 0.0, 0.0]
			cpu memory info: [25206784, 391258112, 12693504, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01870141381655226
	29547:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.230000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [2]
			cpu times: [0.06, 0.03, 0.0, 0.0, 0.0]
			cpu memory info: [25206784, 391258112, 12693504, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01870141381655226
	29579:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.300000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [5]
			cpu times: [0.05, 0.03, 0.0, 0.0, 0.0]
			cpu memory info: [25206784, 391258112, 12693504, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01870141381655226
	29554:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.260000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [3]
			cpu times: [0.05, 0.04, 0.0, 0.0, 0.0]
			cpu memory info: [25202688, 391258112, 12689408, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01869837491277966
	29567:openmpi_3.1_gcc_6.2_app
			created: 2021-03-25 17:18:38.290000
			cmdline: /tmp/lustre_shared/plgkopta/qcgpjm-altair/examples/openmpi_3.1_gcc_6.2_app
			parent: 28521:mpirun
			cpu affinity: [4]
			cpu times: [0.06, 0.03, 0.0, 0.0, 0.0]
			cpu memory info: [25206784, 391258112, 12693504, 8192, 0, 153370624, 0]
			cpu memory percent: 0.01870141381655226	


It is worth to mention, that analysis with the ``qcg-pm-processes`` tool can be done at any time outside the Slurm allocation. The only input data is the working directory.

