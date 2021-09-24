Executor API
============
*Beta*

*Executor API* is an alternative, simplified programming interface for QCG-PilotJob.
In some aspects it mimics an interface of ``concurrent.futures.Executor`` and may therefore
be appealing to many Python programmers. However, since this interface is still under development,
it is dedicated mostly for less-demanding use-cases.

*Executor API* is based on the basic API of QCG-PilotJob and therefore it inherits core elements
from that API. On the other hand, in order to support definition of the common execution scenarios, many elements of
basic API have been hidden behind simplified interface.

Installation
------------
*Executor API* can be installed from PyPi, with the following command:

.. code:: bash

   $ pip install qcg-pilotjob-executor-api

Usage
-----
Before we present more details about the usage of Executor API, let's outline a minimal working example:

.. code-block:: python
    :linenos:

    from qcg.pilotjob.executor_api.qcgpj_executor import QCGPJExecutor
    from qcg.pilotjob.executor_api.templates.basic_template import BasicTemplate

    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='date')
        f.result()

This example shows how `Executor API` can be used to run specific command, here ``date``, within a QCG-PilotJob task.

The interesting part starts on the 4th line. Here we create ``QCGPJExecutor``, which is an entry point to
QCG-PilotJob. Actually, behind the scenes ``QCGPJExecutor`` initialises the QCG-PilotJob manager service
and it plays a role of a proxy to its methods.

Once created, ``QCGPJExecutor`` allows us to submit tasks for the execution within QCG-PilotJob.
An example invocation of the ``submit`` method is shown on the 5th line. The first and the most interesting argument
to this method is template. The template is actually a `Callable` that returns a tuple consisting of
string and dictionary. The string need to be a QCG-PilotJob submit request description written
in a JSON format with optional placeholders for substitution of specific parameters,
while the dictionary may be used to set default values for placeholders.
The next parameters of the method are optional and dependent on the selected template -
their role is to provide values for the actual substitution of placeholders.

In the example above we use a predefined template called ``BasicTemplate.template``, which requires
only two parameters to be provided, namely ``name`` and ``exec``.

The ``submit`` method returns a ``QCGPJFuture`` object, which provides methods associated with the execution
of submission. For instance, the invocation ``f.result()`` in the example above, blocks processing until the task
is not completed and then returns the status of its execution.

QCGPJExecutor
~~~~~~~~~~~~~
``QCGPJExecutor`` is an approximate implementation of the ``concurrent.futures.Executor`` interface, but instead of
execution of functions using threads or multiprocessing module like it takes place in case of python build-in
executors, here we execute QCG-PilotJob's tasks.

Technically, ``QCGPJExecutor`` is a kind of proxy over the QCG-PilotJob manager and at the expense
of some flexibility of the covered service, it provides simpler interface.
``QCGPJExecutor``'s constructor can be invoked without any parameters and then it is started with default settings.

However, in order to enable easy configuration of the commonly changed settings,
several optional parameters are provided. One of such parameters is ``resources`` which may be useful for
testing QCG-PilotJob on a local laptop.

``QCGPJExecutor`` implements `ContextManager`'s methods that allow for its easy usage with the ``with`` statements.
When the ``with`` statement is used, python will automatically take care of releasing ``QCGPJExecutor``'s resources.

When the ``QCGPJExecutor`` is constructed outside the ``with`` statement, it needs to be released manually,
using the ``shutdown`` method.

For the full reference of the ``QCGPJExecutor`` module see :py:mod:`qcg.pilotjob.executor_api.qcgpj_executor`.

Submission of tasks
~~~~~~~~~~~~~~~~~~~
The key method offered by QCGPJExecutor is ``submit``. The call of this method adds a new task (or tasks, depending on
the usage scenario) to the QCG-PilotJob's queue to be executed once resources are available and dependencies satisfied.
The method takes the following arguments:

1. ``fn`` :
    a callable that returns a tuple representing a template. The first element of the tuple should be a
    string containing a QCG-PilotJob submit request expressed in a JSON format compatible
    with the :ref:`QCG-PilotJob's interface <Commands>`. The string can include placeholders
    (identifiers preceded by $ symbol) that are the target for substitution.
    The second element of a tuple is dictionary which may be used to assign default values for
    substitution of selected placeholders.
2. ``*args`` :
    a set of dicts which contain parameters that will be used to substitute placeholders
    defined in the template.
3. ``**kwargs`` :
    a set of keyword arguments that will be used to substitute placeholders defined in
    the template.
**Note**: In the process of substitution ``**kwargs`` overwrite ``*args`` and ``*args`` overwrite defaults

Example template
~~~~~~~~~~~~~~~~
In order to understand how to use or create templates, possibly the best option is to look at the example.
``BasicTemplate`` class, which is delivered with the QCG-PilotJob Executor API, provides a predefined
template method that was already used in the example above. It is a simple example, but can give a good overview.

.. code-block:: python
    :linenos:

    class BasicTemplate(QCGPJTemplate):
        @staticmethod
        def template() -> Tuple[str, Dict[str, Any]]:
            template = """
            {
                'name': '${name}',
                'execution': {
                    'exec': '${exec}',
                    'args': ${args},
                    'stdout': '${stdout}',
                    'stderr': '${stderr}'
                }
            }
             """

            defaults = {
                'args': [],
                'stdout': 'stdout',
                'stderr': 'stderr'
            }

            return template, defaults

Here, accordingly with the expectations, the function returns ``template`` and ``defaults``.
The ``template`` is a JSON dictionary representing a QCG-PilotJob :ref:`submit request <Commands>`. What is
important, it includes a set of ``${}`` placeholders. These placeholders may be substituted by the parameters
provided to the ``submit`` method. For some of the placeholders, default values are already
predefined in a ``defaults`` dictionary, and these parameters don't need to be substituted
if there is no concrete reason for this. The rest of placeholders, namely ``{name}`` and ``{exec}``, don't have
default values and therefore they need to be substituted by parameters provided to the ``submit``.

Let's see how example invocations of the ``submit`` method for this template can look like:

.. code-block:: python

    e.submit(BasicTemplate.template, name='tj', exec='date')
    e.submit(BasicTemplate.template, name='tj', exec='sleep', args=['10'])

QCGPJFuture
~~~~~~~~~~~
The ``submit`` method returns ``QCGPJFuture`` object, which plays a role of a handler for the submission.
Thus, using the returned ``QCGPJFuture`` object it is possible to make queries to check if
the submitted task has been finished, with the ``done`` method,
or request the cancellation of an execution with the ``cancel`` method. As it was presented in the
attached example, it is also possible to invoke blocking wait until the task is finished with the ``result`` method.
For the full reference of methods provided by ``QCGPJFuture`` see :py:mod:`qcg.pilotjob.executor_api.qcgpj_future`.
