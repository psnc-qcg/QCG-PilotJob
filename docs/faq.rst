FAQ
===

How is QCG-PilotJob better than a BASH script?
----------------------------------------------
QCG-PilotJob has been designed to simplify definition of common scenarios of execution
of large number of tasks on computing resources. Typically these scenarios were
done by application developers in a custom and often far from an optimal way.
With QCG-PilotJob users are offered with ready to use efficient mechanisms
as well as nice API that can be recognized as much more natural solution
than sophisticated *BASH* scripts, for both direct human use and integration with other software components.

The particular advantage of QCG-PilotJob is visible in case of dynamic scenarios
with dynamic number of jobs, dynamic requirements of these jobs and a need to start / cancel
these jobs depending on the intermediary results of calculations.
For these scenarios the core capabilities of QCG-PilotJob and easy to use constructs
offered by QCG-PilotJob API seem to be exceptionally sound.

For all kinds of scenarios, also for the static use-cases (where we know in advance a number of tasks,
their requirements, and we have a static allocation) QCG-PilotJob provides a few advantages,
like built-in mechanism to resume prematurely stopped workflow,
tools for collecting timings from the execution and generation of the reports
for the analysis (e.g. Gantt chart), or a custom launcher for single-core tasks,
which is more efficient (at least on some resources) than the *srun* command run from *BASH*.
QCG-PilotJob delivers also different predefined models of running tasks with
`srun`, `intelmpi`, `openmpi` as well as a with `openmp`,
which simplify execution of MPI and OpenMP based applications
across different computing resources.

However, the target powerfulness of the QCG-PilotJob should be achieved when we
release the common queue service that will provide the possibility to combine resources
from many allocations into one QCG-PilotJob. Then it will be easy to extend the resources
depending on the dynamic needs of the scenario, taking them even from many HPC facilities.

How is QCG-PilotJob better than existing Workflow / Pilot Job implementations?
------------------------------------------------------------------------------
The strategic decision for the development of QCG-PilotJob was to ensure simplicity of
the entire process related to the tool's use: from its installation,
through defining workflows, to the actual execution of tasks.
Thus, in contrast to many existing products, QCG-PilotJob not only simplifies
definition of execution scenarios, but also comes very easy to install and can be run
without problems across different environments, even conservative and variously restricted ones.

It should be stressed that QCG-PilotJob is a fully user-space solution, and as such,
can be installed by an ordinary user, in its home directory (e.g. in a virtual environment).
At any step there is no need to bother administrators: to install something or to open some ports.
