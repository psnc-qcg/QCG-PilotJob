from distutils.core import setup

setup(
	name='qcg-pilotjob-api',

	version='0.11.2',

	author='Piotr Kopta',
	author_email='pkopta@man.poznan.pl',

	packages=['qcg.pilotjob.api'],
        package_dir={'qcg.pilotjob.api': 'src/qcg/pilotjob/api'},

	url='http://dokumentacja',

	description='API for QCG PilotJob Manager',

	install_requires=[
		'zmq',
		],
)


