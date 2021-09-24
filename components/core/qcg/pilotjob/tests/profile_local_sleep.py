import sys

from tempfile import TemporaryDirectory
from os.path import join, abspath

from qcg.pilotjob.service import QCGPMService
from qcg.pilotjob.tests.utils import save_reqs_to_file
from qcg.pilotjob.tests.job_stats import analyze_job_report

from shutil import copyfile


if 'profile' in globals():
    print('running with line_profiler')
else:
    print('running without line_profiler')


with TemporaryDirectory() as tmpdir:
    file_path = join(tmpdir, 'jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = "sleep-iter_${it}"
    nits = 1000
    jobSleepTime = 1
    jobCores = 1
    availCores = 50
    rounds = nits * jobCores / availCores
    totalExecTime = rounds * jobSleepTime
    jobs = [
        {
            "name": jobName,
            "iteration": { 'start': 0, 'stop': nits },
            "execution": {
                "exec": "/bin/sleep",
                "args": ["{}s".format(str(jobSleepTime))],
#                "wd": abspath(join(tmpdir, jobName)),
#                "stdout": "sleep-iter.stdout",
#                "stderr": "sleep-iter.stderr"
            },
            "resources": {
                "numCores": {
                    "exact": jobCores,
                }
            }
        }
    ]
    reqs = [{'request': 'submit', 'jobs': jobs},
            {'request': 'control', 'command': 'finishAfterAllTasksDone'}]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    print('starting scenario with {} jobs, anticipated execution time: {}'.format(nits, totalExecTime))

    sys.argv = ['QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', str(availCores), '--wd', str(tmpdir),
                '--report-format', 'json', '--log', 'error']
    QCGPMService().start()

    copyfile(join(tmpdir, 'jobs.report'), join('.', 'jobs.report.json'))
    copyfile(join(tmpdir, 'service.log'), join('.', 'service.log'))
    analyze_job_report(join('.', 'jobs.report.json'))