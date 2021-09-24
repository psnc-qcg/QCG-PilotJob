import versioneer

from setuptools import setup

with open("README.md", "r") as fh:
     long_description = fh.read()

setup(
    name="qcg-pilotjob-cmds",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),

    author="Piotr Kopta",
    author_email="pkopta@man.poznan.pl",

    packages=[
        "qcg.pilotjob.cmds",
    ],

    url="http://github.com/vecma-project/QCG-PilotJob",

    description="Manage many jobs inside one allocation",
    long_description=long_description,
    long_description_content_type="text/markdown",

    install_requires=[
        "qcg-pilotjob",
        "termcolor",
        "prompt_toolkit",
        "plotly_express>=0.4.1",
        "pandas",
        "kaleido"
    ],

    entry_points = {
        'console_scripts': [
            'qcg-pm-report=qcg.pilotjob.cmds.report:reports',
            'qcg-pm-processes=qcg.pilotjob.cmds.processes:processes'
        ]
    }
)
