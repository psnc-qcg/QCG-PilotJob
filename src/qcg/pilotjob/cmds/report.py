import click
import sys
import os
import statistics

from qcg.pilotjob.utils.auxdir import find_single_aux_dir, find_log_files, find_report_files
from qcg.pilotjob.utils.reportstats import JobsReportStats


def print_stats(stats):
    jstats = stats.job_stats()

    if jstats:
        print('{} jobs executed in {} secs'.format(len(jstats.get('jobs', {})),
            jstats['total_time'].total_seconds() if jstats.get('total_time') else 0))
        print('\t{:>20}: {}'.format('first job queued', str(jstats.get('first_queue', 0))))
        print('\t{:>20}: {}'.format('last job queued', str(jstats.get('last_queue', 0))))
        print('\t{:>20}: {}'.format('total queuing time', jstats['queue_time'].total_seconds() if jstats.get('queue_time') else 0))
        print('\t{:>20}: {}'.format('first job start', str(jstats.get('first_start', 0))))
        print('\t{:>20}: {}'.format('last job finish', str(jstats.get('last_finish', 0))))
        print('\t{:>20}: {}'.format('total execution time', jstats['execution_time'].total_seconds() if jstats.get('execution_time') else 0))

        print('jobs runtime statistics:')
        for k, v in jstats.get('rstats', {}).items():
            print('\t{:>20}: {}'.format(k, v))

        print('jobs launching statistics:')
        for k, v in jstats.get('launchstats', {}).items():
            print('\t{:>20}: {}'.format(k, v))

    res = stats.resources()
    if res:
        print('available resources:')
        for k, v in res.items():
            print('\t{:>20}: {}'.format(k, v))


def print_text(stats):
    jstats = stats.job_stats()

    if jstats:
        for jname, job in jstats.get('jobs', {}).items():
            jstate = job.get('state')
            jmessages = job.get('messages')
            jhistory = job.get('history')
            jruntime = job.get('runtime')

            print("{} ({}) {}\n\t{}\n\t{}\n".format(
                jname, jstate, jmessages or '',
                "\n\t".join(["{}: {}".format(en.get("date"), en.get("state")) for en in jhistory]),
                "\n\t".join(["{}: {}".format(k, v) for k, v in jruntime.items()])))


@click.group()
def reports():
    pass


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--verbose', is_flag=True, default=False)
def stats(wdir, verbose):
    jobs_report_path = find_report_files(wdir)
    if verbose:
        print(f'found report files: {jobs_report_path} ...')
    log_files = find_log_files(wdir)
    if verbose:
        print(f'found log files: {log_files} ...')

    stats = JobsReportStats(jobs_report_path, log_files, verbose)
    print_stats(stats)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--verbose', is_flag=True, default=False)
def text(wdir, verbose):
    jobs_report_path = find_report_files(wdir)

    if verbose:
        print(f'found report files: {jobs_report_path} ...')

    stats = JobsReportStats(jobs_report_path, None, verbose)
    print_text(stats)


if __name__ == '__main__':
    reports()
