import argparse
import json
import sys
import statistics
import traceback
import re

from qcg.pilotjob.utils.auxdir import find_single_aux_dir, find_report_files, find_log_files
from qcg.pilotjob.utils.util import parse_datetime

from json import JSONDecodeError
from datetime import datetime, timedelta
from os.path import exists, join, abspath, isdir
from os import listdir
from math import ceil



class JobsReportStats:

    def __init__(self, report_files, log_files=None, verbose=False):
        """
        Analyze QCG-PJM execution.

        Args:
            report_files (list(str)) - list of paths to the report files
            log_files (list(str)) - list of paths to the log files
        """
        self.report_files = report_files
        self.log_files = log_files or []
        self.verbose = verbose

        self.jstats = {}
        self.res = {}

        self._analyze()

    def job_stats(self):
        return self.jstats

    def resources(self):
        return self.res

    def _analyze(self):
        self._read_report_files(self.report_files)

        if self.log_files:
            self._parse_service_logs(self.log_files)
        
    def _read_report_files(self, report_files):
        """
        Read QCG-PJM json report file.
        The read data with statistics data are written to the self.jstats dictionary.

        Args:
            report_file (str) - path to the QCG-PJM json report file
        """
        self.jstats = {'jobs': {}}
        min_queue, max_queue, min_start, max_finish = None, None, None, None

        if self.verbose:
            print(f'reading reports from {",".join(report_files)} files ...')

        for report_file in report_files:
            if self.verbose:
                print('parsing report file {} ...'.format(report_file))

            with open(report_file, 'r') as report_f:

                for line, entry in enumerate(report_f, 1):
                    try:
                        job_entry = json.loads(entry)
                    except JSONDecodeError as e:
                        raise Exception('wrong report "{}" file format: error in {} line: {}'.format(report_file, line, str(e)))

                    for attr in [ 'name', 'state', 'history', 'runtime' ]:
                        if not attr in job_entry:
                            raise Exception('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_file, attr))

                    rtime = None
                    if 'rtime' in job_entry['runtime']:
                        rtime_t = datetime.strptime(job_entry['runtime']['rtime'], "%H:%M:%S.%f")
                        rtime = timedelta(hours=rtime_t.hour, minutes=rtime_t.minute, seconds=rtime_t.second, microseconds=rtime_t.microsecond)

                    # find queued time
                    queued_state = list(filter(lambda st_en: st_en['state'] == 'QUEUED', job_entry['history']))

                    # find allocation creation time
                    schedule_state = list(filter(lambda st_en: st_en['state'] == 'SCHEDULED', job_entry['history']))

                    # find start executing time
                    exec_state = list(filter(lambda st_en: st_en['state'] == 'EXECUTING', job_entry['history']))

                    # find finish executing time
                    finish_state = list(filter(lambda st_en: st_en['state'] == 'SUCCEED', job_entry['history']))
                    assert len(finish_state) == 1, 'for job {} in line {}'.format(job_entry['name'], line)

                    queued_time = parse_datetime(queued_state[0]['date']) if queued_state else None
                    schedule_time = parse_datetime(schedule_state[0]['date']) if schedule_state else None
                    start_time = parse_datetime(exec_state[0]['date']) if exec_state else None
                    finish_time = parse_datetime(finish_state[0]['date']) if finish_state else None

                    self.jstats['jobs'][job_entry['name']] = {
                        'r_time': rtime,
                        'queue_time': queued_time,
                        'sched_time': schedule_time,
                        's_time': start_time,
                        'f_time': finish_time,
                        'name': job_entry['name'],
                        'pid': job_entry['runtime'].get('pid', None),
                        'pname': job_entry['runtime'].get('pname', None),
                        'runtime': job_entry['runtime'],
                        'history': job_entry['history'],
                        'state': job_entry['state'],
                        'messages': job_entry.get('messages'),
                    }

                    if queued_time:
                        if not min_queue or queued_time < min_queue:
                            min_queue = queued_time

                        if not max_queue or queued_time > max_queue:
                            max_queue = queued_time

                    if start_time:
                        if not min_start or start_time < min_start:
                            min_start = start_time

                        if not max_finish or finish_time > max_finish:
                            max_finish = finish_time

        self.jstats['first_queue'] = min_queue
        self.jstats['last_queue'] = max_queue
        self.jstats['queue_time'] = max_queue - min_queue if all((max_queue is not None, min_queue is not None)) else None
        self.jstats['first_start'] = min_start
        self.jstats['last_finish'] = max_finish
        self.jstats['execution_time'] = max_finish - min_start if all((max_finish is not None, min_start is not None)) else None
        self.jstats['total_time'] = max_finish - min_queue if all((max_finish is not None, min_queue is not None)) else None

        rtimes = [job['r_time'].total_seconds() for job in self.jstats['jobs'].values() if job['r_time']]

        launchtimes = [(job['s_time'] - job['sched_time']).total_seconds() for job in self.jstats['jobs'].values() if job['s_time'] and job['sched_time']]

        if rtimes:
            self.jstats['rstats'] = self._generate_series_stats(rtimes)

        if launchtimes:
            self.jstats['launchstats'] = self._generate_series_stats(launchtimes)

    def _generate_series_stats(self, serie):
        """
        Generate statistics about given data serie.

        Args:
            serie (float[]) - serie data

        Return:
            serie statistics in form of dictionary
        """
        stats = {}
        stats['max'] = max(serie)
        stats['min'] = min(serie)
        stats['mean'] = statistics.mean(serie)
        stats['median'] = statistics.median(serie)
        stats['median_lo'] = statistics.median_low(serie)
        stats['median_hi'] = statistics.median_high(serie)
        stats['stdev'] = statistics.stdev(serie)
        stats['pstdev'] = statistics.pstdev(serie)
        stats['var'] = statistics.variance(serie)
        stats['pvar'] = statistics.pvariance(serie)
        return stats

    def _parse_service_logs(self, service_log_files):
        res_regexp = re.compile('available resources: (\d+) \((\d+) used\) cores on (\d+) nodes')
        gov_regexp = re.compile('starting governor manager ...')
        resinfo_regexp = re.compile('selected (\w+) resources information')
        self.res = {}

        log_file = None

        if len(service_log_files) > 1:
            governor_logs = []

            # find log of governor manager
            for log_file in service_log_files:
                with open(log_file, 'r') as l_f:
                    for line in l_f:
                        m = gov_regexp.search(line.strip())
                        if m:
                            # found governor log
                            governor_logs.append(log_file)
                            break

            if len(governor_logs) != 1:
                print('warning: can not find single governor log (found files: {})'.format(','.join(governor_logs)))
                print('warning: selecting the first log file: {}'.format(service_log_files[0]))
                log_file = service_log_files[0]
            else:
                log_file = governor_logs[0]
        else:
            if service_log_files:
                log_file = service_log_files[0]

        if self.verbose:
            print('parsing log file {} ...'.format(log_file))

        with open(log_file, 'r') as s_f:
            for line in s_f:
                m = res_regexp.search(line.strip())
                if m:
                    self.res = { 'cores': int(m.group(1)) - int(m.group(2)), 'nodes': int(m.group(3)) }
                    if self.verbose:
                        print('found resources: {}'.format(str(self.res)))
                    break

    def print_stats(self):
        if self.jstats:
            print('{} jobs executed in {} secs'.format(len(self.jstats.get('jobs', {})),
                self.jstats['total_time'].total_seconds() if 'total_time' in self.jstats else 0))
            print('\t{:>20}: {}'.format('first job queued', str(self.jstats.get('first_queue', 0))))
            print('\t{:>20}: {}'.format('last job queued', str(self.jstats.get('last_queue', 0))))
            print('\t{:>20}: {}'.format('total queuing time', self.jstats['queue_time'].total_seconds() if 'queue_time' in self.jstats else 0))
            print('\t{:>20}: {}'.format('first job start', str(self.jstats.get('first_start', 0))))
            print('\t{:>20}: {}'.format('last job finish', str(self.jstats.get('last_finish', 0))))
            print('\t{:>20}: {}'.format('total execution time', self.jstats['execution_time'].total_seconds() if 'execution_time' in self.jstats else 0))
            
            print('jobs runtime statistics:')
            for k, v in self.jstats.get('rstats', {}).items():
                print('\t{:>20}: {}'.format(k, v))

            print('jobs launching statistics:')
            for k, v in self.jstats.get('launchstats', {}).items():
                print('\t{:>20}: {}'.format(k, v))

        if self.res:
            print('available resources:')
            for k, v in self.res.items():
                print('\t{:>20}: {}'.format(k, v))

