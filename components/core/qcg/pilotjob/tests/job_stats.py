import json
import statistics

from json import JSONDecodeError
from datetime import datetime, timedelta

def analyze_job_report(report_path):
    jstats = {}

    min_start, max_finish = None, None

    with open(report_path, 'r') as report_f:
        for line, entry in enumerate(report_f, 1):
            try:
                job_entry = json.loads(entry)
            except JSONDecodeError as e:
                raise ValueError('wrong jobs.report {} file format: error in {} line: {}'.format(report_path, line, str(e)))

            for attr in [ 'name', 'state', 'history', 'runtime' ]:
                if not attr in job_entry:
                    raise ValueError('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_path, attr))

            rtime_t = datetime.strptime(job_entry['runtime']['rtime'], "%H:%M:%S.%f")
            rtime = timedelta(hours=rtime_t.hour, minutes=rtime_t.minute, seconds=rtime_t.second, microseconds=rtime_t.microsecond)

            # find start executing time
            exec_state = list(filter(lambda st_en: st_en['state'] == 'EXECUTING', job_entry['history']))
            assert len(exec_state) == 1

            # find finish executing time
            finish_state = list(filter(lambda st_en: st_en['state'] == 'SUCCEED', job_entry['history']))
            assert len(finish_state) == 1

            start_time = datetime.strptime(exec_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')
            finish_time = datetime.strptime(finish_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')

            jstats[job_entry['name']] = { 'r_time': rtime, 's_time': start_time, 'f_time': finish_time }

            if not min_start or start_time < min_start:
                min_start = start_time

            if not max_finish or finish_time > max_finish:
                max_finish = finish_time

    scenario_duration = finish_time - min_start

    print('read {} jobs'.format(len(jstats)))
    print('scenario execution time: {}'.format(scenario_duration.total_seconds()))

    rtimes = [ job['r_time'].total_seconds() for job in jstats.values() ]
    print('runtime:')
    print('\tmax: {}'.format(max(rtimes)))
    print('\tmean: {}'.format(statistics.mean(rtimes)))
    print('\tmedian: {}'.format(statistics.median(rtimes)))
    print('\tmedian_low: {}'.format(statistics.median_low(rtimes)))
    print('\tmedian_high: {}'.format(statistics.median_high(rtimes)))
    print('\tstd dev: {}'.format(statistics.stdev(rtimes)))
    print('\tpolulation std dev: {}'.format(statistics.pstdev(rtimes)))
    print('\tvariance: {}'.format(statistics.variance(rtimes)))
    print('\tpopulation variance: {}'.format(statistics.pvariance(rtimes)))
