from setuptools import setup

with open("README.md", "r") as fh:
	long_description = fh.read()

setup(
	name="qcg-pilotjob",
	version="0.14.0-beta",

	author="Piotr Kopta",
	author_email="pkopta@man.poznan.pl",

	packages=["qcg.pilotjob",
		  "qcg.pilotjob.utils",
		  "qcg.pilotjob.common",
		  "qcg.pilotjob.executor",
		  "qcg.pilotjob.executor.cmd",
		  "qcg.pilotjob.executor.launcher",
		  "qcg.pilotjob.inputqueue",
		  "qcg.pilotjob.inputqueue.cmd"],
	package_dir={
		  "qcg.pilotjob": "src/qcg/pilotjob",
		  "qcg.pilotjob.utils": "src/qcg/pilotjob/utils",
		  "qcg.pilotjob.common": "src/qcg/pilotjob/common",
		  "qcg.pilotjob.executor": "src/qcg/pilotjob/executor",
		  "qcg.pilotjob.executor.cmd": "src/qcg/pilotjob/executor/cmd",
		  "qcg.pilotjob.executor.launcher": "src/qcg/pilotjob/executor/launcher",
		  "qcg.pilotjob.inputqueue": "src/qcg/pilotjob/inputqueue",
		  "qcg.pilotjob.inputqueue.cmd": "src/qcg/pilotjob/inputqueue/cmd"
	},

	url="http://github.com/vecma-project/QCG-PilotJob",

	description="Manage many jobs inside one allocation",
	long_description=long_description,
	long_description_content_type="text/markdown",

	install_requires=[
		"zmq",
		"click",
		"prompt_toolkit",
		"cached-property"
		],

	entry_points = {
		'console_scripts': ['qcg-service=qcg.pilotjob.inputqueue.cmd.service:iq',
				    'qcg-executor=qcg.pilotjob.executor.cmd.service:ex',
				    'qcg-client=qcg.pilotjob.inputqueue.cmd.client:client'],
	},
)
