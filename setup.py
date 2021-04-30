from setuptools import setup

with open("README.md", "r") as fh:
	long_description = fh.read()

setup(
	name="qcg-pilotjob",
	version="0.11.2",

	author="Piotr Kopta",
	author_email="pkopta@man.poznan.pl",

	packages=[
            "qcg.pilotjob",
            "qcg.pilotjob.api",
            "qcg.pilotjob.launcher",
            "qcg.pilotjob.utils",
            "qcg.pilotjob.cmds",
            ],
	package_dir={
		"qcg.pilotjob": "src/qcg/pilotjob",
		"qcg.pilotjob.api": "src/qcg/pilotjob/api",
		"qcg.pilotjob.launcher": "src/qcg/pilotjob/launcher",
		"qcg.pilotjob.utils": "src/qcg/pilotjob/utils",
		"qcg.pilotjob.cmds": "src/qcg/pilotjob/cmds",
	},

	url="http://github.com/vecma-project/QCG-PilotJob",

	description="Manage many jobs inside one allocation",
	long_description=long_description,
	long_description_content_type="text/markdown",

	install_requires=[
		"zmq",
		"click",
		"prompt_toolkit",
                "psutil",
                "termcolor"
		],

	entry_points = {
		'console_scripts': [
                    'qcg-pm-service=qcg.pilotjob.command_line:service',
                    'qcg-pm=qcg.pilotjob.client_cmd:qcgpjm',
                    'qcg-pm-report=qcg.pilotjob.cmds.report:reports',
                    'qcg-pm-processes=qcg.pilotjob.cmds.processes:processes',
                ],
	},
)
