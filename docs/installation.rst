Installation
============

QCG-PilotJob requires Python version >= 3.6.

All QCG-PilotJob components can be installed by a regular user (without administrative privileges)
In the presented instructions we assume such type of installation.

Preparation of virtualenv (optional step)
-----------------------------------------
In order to make dependency management easier, a good practice is to
install QCG-PilotJob into a fresh virtual environment. To do so, we need the latest
version of *pip* package manager and *virtualenv*. They can be installed in
user's directory by the following commands:

.. code:: bash

    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python3 get-pip.py --user
    pip install --user virtualenv

To create private virtual environment for installed packages, type
the following commands:

.. code:: bash

    virtualenv venv
    . venv/bin/activate

Installation of QCG-PilotJob packages
-------------------------------------
There are two options for the actual installation of QCG-PilotJob packages. You can use the PyPi repository
or install the packages from GitHub.

PyPi
^^^^
The installation of **QCG-PilotJob Core** package from the PyPi repository is as simple as:

.. code:: bash

    pip install qcg-pilotjob

In a similar way you can install supplementary packages, namely
*QCG-PilotJob Command Line Tools* and *QCG-PilotJob Executor API*:

.. code:: bash

    pip install qcg-pilotjob-cmds
    pip install qcg-pilotjob-executor-api

GitHub
^^^^^^

To install QCG-PilotJob packages directly from github.com you can use the following commands:

.. code:: bash

    pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git#subdirectory=components/core
    pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git#subdirectory=components/cmds
    pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git#subdirectory=components/executor_api

You can also install the packages from a specific branch:

.. code:: bash

    pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git@branch_name#subdirectory=components/core
    pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git@branch_name#subdirectory=components/cmds
    pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git@branch_name#subdirectory=components/executor_api
