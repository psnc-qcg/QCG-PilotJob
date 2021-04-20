import json
import statistics
import collections
import re

from qcg.pilotjob.utils.auxdir import find_report_files, find_log_files, find_rtimes_files, find_final_status_files
from qcg.pilotjob.utils.util import parse_datetime

from json import JSONDecodeError
from datetime import datetime, timedelta


class JobsReportStats:

    @staticmethod
    def from_workdir(workdir, verbose=False):
        jobs_report_path = find_report_files(workdir)
        if verbose:
            print(f'found report files: {",".join(jobs_report_path)}')
        log_files = find_log_files(workdir)
        if verbose:
            print(f'found log files: {",".join(log_files)}')
        rt_files = find_rtimes_files(workdir)
        if verbose:
            print(f'found real time log files: {",".join(rt_files)}')
        final_files = find_final_status_files(workdir)
        if verbose:
            print(f'found final status log files: {",".join(final_files)}')

        return JobsReportStats(jobs_report_path, log_files, rt_files, final_files, verbose)

    def __init__(self, report_files, log_files=None, rt_files=None, final_files=None, verbose=False):
        """
        Analyze QCG-PJM execution.

        Args:
            report_files (list(str)) - list of paths to the report files
            log_files (list(str)) - list of paths to the log files
        """
        self.report_files = report_files
        self.log_files = log_files or []
        self.rt_files = rt_files or []
        self.final_files = final_files or []

        self.verbose = verbose

        self.jstats = {}
        self.gstats = {}
        self.res = {}

        self._analyze()

    def has_realtime_stats(self):
        return self.rt_files and all(self.jstats.get(attr) for attr in ['min_real_start',
            'max_real_finish', 'total_real_time'])

    def job_stats(self):
        return self.jstats

    def global_stats(self):
        return self.gstats

    def resources(self):
        return self.res

    def _analyze(self):
        self._read_report_files(self.report_files)

        if self.log_files:
            self._parse_service_logs(self.log_files)

        if self.rt_files:
            self._parse_rt_logs(self.rt_files)

        if self.final_files:
            self._parse_final_files(self.final_files)

    @staticmethod
    def _parse_allocation(allocation):
        nodes = {}
        if allocation:
            for node in allocation.split(','):
                nodes[node[:node.index('[')]] = node[node.index('[') + 1:-1].split(':')

        return nodes

    def _read_report_files(self, report_files):
        """
        Read QCG-PJM json report file.
        The read data with statistics data are written to the self.jstats dictionary.

        Args:
            report_files (list(str)) - list of paths to the QCG-PJM json report file
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
                    finish_state = list(filter(lambda st_en: st_en['state'] in ['SUCCEED','FAILED'], job_entry['history']))
                    assert len(finish_state) == 1, 'for job {} in line {}'.format(job_entry['name'], line)

                    job_nodes = None
                    allocation = job_entry.get('runtime', {}).get('allocation', None)
                    if allocation is not None:
                        job_nodes = JobsReportStats._parse_allocation(allocation)
#                        print(f'found allocation for job {job_entry["name"]}: {allocation}, after parsed: {job_nodes}')

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
                        'nodes': job_nodes,
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

    def _parse_rt_logs(self, rt_logs):
        min_real_start = None
        max_real_finish = None

        rt_jobs = 0

        for rt_log in rt_logs:
            try:
                if self.verbose:
                    print(f'reading real time log file {rt_log} ...')

                with open(rt_log, 'rt') as rt_file:
                    node_rtimes = json.load(rt_file)

                if 'rt' not in node_rtimes:
                    raise ValueError('wrong format - missing "rt" element')

                rtimes = node_rtimes.get('rt', {})
                for job_name, job_rtimes in rtimes.items():
                    if all((elem in job_rtimes for elem in ['s', 'f'])):
                        job_data = self.jstats.setdefault('jobs', {}).setdefault(job_name, {})

                        job_real_start = parse_datetime(job_rtimes.get('s'))
                        job_real_finish = parse_datetime(job_rtimes.get('f'))

                        job_data['real_start'] = job_real_start
                        job_data['real_finish'] = job_real_finish

                        if min_real_start is None or job_real_start < min_real_start:
                            min_real_start = job_real_start

                        if max_real_finish is None or job_real_finish > max_real_finish:
                            max_real_finish = job_real_finish

                        rt_jobs += 1
                    else:
                        if self.verbose:
                            print(f'warning: missing required elements for job {job_name} in rt log file {rt_log}')
            except Exception as exc:
                print(f'warning: can not read real time log file {rt_log}: {str(exc)}')

        if self.verbose:
            print(f'read {rt_jobs} jobs real time entries')

        if min_real_start is not None:
            self.jstats['min_real_start'] = min_real_start

        if max_real_finish is not None:
            self.jstats['max_real_finish'] = max_real_finish

        if all((min_real_start is not None, max_real_finish is not None)):
            self.jstats['total_real_time'] = (max_real_finish - min_real_start).total_seconds()

    def _parse_final_files(self, final_logs):

        global_service_started = None
        global_service_finished = None

        global_nodes = 0
        global_cores = 0
        global_jobs = 0
        global_iterations = 0
        global_failed_jobs = 0
        global_failed_iterations = 0

        for final_log in final_logs:
            try:
                with open(final_log, 'rt') as final_file:
                    final_report = json.load(final_file)

                if all(attr in final_report.get('System', {}) for attr in ['Started', 'Generated']):
                    service_started = parse_datetime(final_report['System']['Started'])
                    service_finished = parse_datetime(final_report['System']['Generated'])

                    if global_service_started is None or service_started < global_service_started:
                        global_service_started = service_started
                    if global_service_finished is None or service_finished > global_service_finished:
                        global_service_finished = service_finished

                global_nodes += final_report.get('Resources', {}).get('TotalNodes', 0)
                global_cores += final_report.get('Resources', {}).get('TotalCores', 0)
                global_jobs += final_report.get('JobStats', {}).get('TotalJobs', 0)
                global_failed_jobs += final_report.get('JobStats', {}).get('FailedJobs', 0)
                global_iterations += final_report.get('IterationStats', {}).get('TotalIterations', 0)
                global_failed_iterations += final_report.get('IterationStats', {}).get('FailedIterations', 0)

            except Exception as ex:
                print(f'warning: failed to read final status log file {final_log}: {str(ex)}')

        if all((global_service_started is not None, global_service_finished is not None)):
            self.gstats['service_start'] = global_service_started
            self.gstats['service_finish'] = global_service_finished

        self.gstats['total_nodes'] = global_nodes
        self.gstats['total_cores'] = global_cores
        self.gstats['total_jobs'] = global_jobs
        self.gstats['failed_jobs'] = global_failed_jobs
        self.gstats['total_iterations'] = global_iterations
        self.gstats['failed_iterations'] = global_failed_iterations

    def job_info(self, *job_ids):
        return {job_id: self.jstats.get('jobs', {}).get(job_id) for job_id in job_ids}

    def filter_jobs(self, filter_def):
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if filter_def(job_data):
                yield job_data

    def allocation_jobs(self, node_name, core_name):
        jobs = []
        
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            job_nodes = job_data.get('nodes')
            if job_nodes and core_name in job_nodes.get(node_name, []):
                jobs.append(job_data)

        jobs.sort(key=lambda job: job['real_start'])
        return jobs

    def job_start_finish_launch_overheads(self, details=False):
        total_start_overhead = 0
        total_finish_overhead = 0
        total_jobs = 0
        total_real_runtime = 0
        total_qcg_runtime = 0
        total_job_overhead_per_runtime = 0

        result = {}
        for job_name, job_data in self.jstats['jobs'].items():
            if all((elem in job_data for elem in ['real_start', 'real_finish', 's_time', 'f_time'])):
                real_job_start = job_data['real_start']
                real_job_finish = job_data['real_finish']
                qcg_job_start = job_data['s_time']
                qcg_job_finish = job_data['f_time']

                start_overhead = (real_job_start - qcg_job_start).total_seconds()
                finish_overhead = (qcg_job_finish - real_job_finish).total_seconds()

                job_real_runtime = (real_job_finish - real_job_start).total_seconds()
                job_qcg_runtime = (qcg_job_finish - qcg_job_start).total_seconds()

                total_real_runtime += job_real_runtime
                total_qcg_runtime += job_qcg_runtime

                if details:
                    result.setdefault('jobs', {})[job_name] = {'start': start_overhead, 'finish': finish_overhead}

                total_start_overhead += start_overhead
                total_finish_overhead += finish_overhead

#                print(f'job {job_name} overhead: {(start_overhead + finish_overhead)}')
#                print(f'job {job_name} runtime: {job_real_runtime}')
#                print(f'job {job_name} overhead per runtime %: {100.0 * ((start_overhead + finish_overhead) / job_real_runtime)}')
                total_job_overhead_per_runtime += 100.0 * ((start_overhead + finish_overhead) / job_real_runtime)
                total_jobs += 1

        if self.verbose:
            print('generated start/finish launch overheads for {total_jobs} jobs')

        result['start'] = total_start_overhead
        result['finish'] = total_finish_overhead
        result['total'] = total_start_overhead + total_finish_overhead
        result['job_start_avg'] = total_start_overhead/total_jobs if total_jobs else 0
        result['job_finish_avg'] = total_finish_overhead/total_jobs if total_jobs else 0
        result['job_avg'] = (total_start_overhead + total_finish_overhead)/total_jobs if total_jobs else 0
        result['job_real_rt_avg'] = (total_real_runtime)/total_jobs if total_jobs else 0
        result['job_qcg_rt_avg'] = (total_qcg_runtime)/total_jobs if total_jobs else 0
        result['job_avg_per_rt'] = (total_job_overhead_per_runtime)/total_jobs if total_jobs else 0
        result['analyzed_jobs'] = total_jobs

        return result

    def _generate_gantt_dataframe(self, start_metric_name, finish_metric_name):
        jobs_chart = []

        min_start = None
        max_finish = None

        avail_nodes = collections.OrderedDict()
        total_jobs = 0

        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if all(job_data.get(elem) is not None for elem in ['nodes', start_metric_name, finish_metric_name]):
                total_jobs += 1

                if min_start is None or job_data.get(start_metric_name) < min_start:
                    min_start = job_data.get(start_metric_name)
                if max_finish is None or job_data.get(finish_metric_name) > max_finish:
                    max_finish = job_data.get(finish_metric_name)

                for node_name, cores in job_data.get('nodes', {}).items():
                    avail_nodes.setdefault(node_name, set()).update(int(core) for core in cores)

                    jobs_chart.extend([{'Job': job_name,
                                        'Start': str(job_data.get(start_metric_name)),
                                        'Finish': str(job_data.get(finish_metric_name)),
                                        'Core': f'{node_name}:{core}'} for core in cores])

        total_nodes = len(avail_nodes)
        total_cores = sum(len(cores) for _, cores in avail_nodes.items())
        total_seconds = (max_finish - min_start).total_seconds()
        node_order = []
        while len(avail_nodes) > 0:
            node_name, cores = avail_nodes.popitem(last=False)
            node_order.extend([f'{node_name}:{core}' for core in sorted(cores)])

        return {'chart_data': jobs_chart,
                'total_jobs': total_jobs,
                'total_nodes': total_nodes,
                'total_cores': total_cores,
                'total_seconds': total_seconds,
                'node_order': node_order}

    def _generate_gantt_gaps_dataframe(self, start_metric_name, finish_metric_name):
        gaps_chart = []

        min_start = None
        max_finish = None

        avail_nodes = collections.OrderedDict()
        total_jobs = 0

        resource_nodes = {}

        # assign jobs to cores to compute time boundaries
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if all(job_data.get(elem) is not None for elem in ['nodes', start_metric_name, finish_metric_name]):
                total_jobs += 1

                if min_start is None or job_data.get(start_metric_name) < min_start:
                    min_start = job_data.get(start_metric_name)
                if max_finish is None or job_data.get(finish_metric_name) > max_finish:
                    max_finish = job_data.get(finish_metric_name)

                for node_name, cores in job_data.get('nodes', {}).items():
                    avail_nodes.setdefault(node_name, set()).update(int(core) for core in cores)

                    for core in cores:
                        resource_nodes.setdefault(node_name, {}).setdefault(core, []).append(job_data)

        total_nodes = len(avail_nodes)
        total_cores = sum(len(cores) for _, cores in avail_nodes.items())
        total_seconds = (max_finish - min_start).total_seconds()

        # sort jobs in each of the core by the start time
        for node_name, cores in resource_nodes.items():
            for core_name, core_jobs in cores.items():
                core_jobs.sort(key=lambda job: job[start_metric_name])

                if core_jobs:
                    if core_jobs[0][start_metric_name] != min_start:
                        gaps_chart.append({'Job': 'gap',
                                           'Start': min_start,
                                           'Finish': core_jobs[0][start_metric_name],
                                           'Core': f'{node_name}:{core_name}'})

                    for job_nr in range(1,len(core_jobs)):
                        curr_job = core_jobs[job_nr]
                        prev_job = core_jobs[job_nr-1]

                        gaps_chart.append({'Job': 'gap',
                                           'Start': prev_job[finish_metric_name],
                                           'Finish': curr_job[start_metric_name],
                                           'Core': f'{node_name}:{core_name}'})

                    if core_jobs[-1][finish_metric_name] != max_finish:
                        gaps_chart.append({'Job': 'gap',
                                           'Start': core_jobs[-1][finish_metric_name],
                                           'Finish': max_finish,
                                           'Core': f'{node_name}:{core_name}'})
                else:
                    gaps_chart.append({'Job': 'gap',
                                       'Start': min_start,
                                       'Finish': max_finish,
                                       'Core': f'{node_name}:{core_name}'})

        node_order = []
        while len(avail_nodes) > 0:
            node_name, cores = avail_nodes.popitem(last=False)
            node_order.extend([f'{node_name}:{core}' for core in sorted(cores)])

        return {'chart_data': gaps_chart,
                'total_jobs': total_jobs,
                'total_nodes': total_nodes,
                'total_cores': total_cores,
                'total_seconds': total_seconds,
                'node_order': node_order}

    def gantt(self, output_file, real=True):
        self._generate_gantt_chart(output_file, self._generate_gantt_dataframe, real=real)

    def gantt_gaps(self, output_file, real=True):
        self._generate_gantt_chart(output_file, self._generate_gantt_gaps_dataframe, real=real)

    def _generate_gantt_chart(self, output_file, dataframe_generator, real=True):

        try:
            import plotly.express as px
            import pandas as pd
        except ImportError:
            raise ImportError('To generate gantt chart the following packages must be installed: '
                              'plotly.express, pandas, kaleido')

        start_metric_name = 's_time'
        finish_metric_name = 'f_time'
        if real:
            start_metric_name = 'real_start'
            finish_metric_name = 'real_finish'

        chart_data = dataframe_generator(start_metric_name, finish_metric_name)

        if self.verbose:
            print(f'generated dataframes for {chart_data.get("total_jobs")} jobs on {chart_data.get("total_cores")} cores')
            print(f'total nodes {chart_data.get("total_nodes")}, total seconds {chart_data.get("total_seconds")}')

        min_start_moment = self.jstats.get('min_real_start')
        max_finish_moment = self.jstats.get('max_real_finish')

#        print(f'node order: {chart_data.get("node_order")}')
        df = pd.DataFrame(chart_data.get("chart_data"))
        fig = px.timeline(df, x_start='Start', x_end='Finish', y='Core', color='Job', category_orders={'Core': chart_data.get('node_order')})
        fig.update_layout(
                autosize=False,
                width=int(chart_data.get('total_seconds', 1))*20,
                height=chart_data.get('total_cores', 1)*20,
                yaxis=dict(
                    title_text="Cores",
                    ticktext=chart_data.get("node_order"),
#                    tickvals=[1, 2, 3, 4],
                    tickmode="array"
                ))
        fig.write_image(output_file)

    def resource_usage(self, details=False):
        resource_nodes = {}
        jobs = {}
        report = {}

        # assign jobs to cores
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if job_data.get('nodes') is not None and job_data.get('real_start') is not None:
                for node_name, cores in job_data.get('nodes', {}).items():
                    for core in cores:
                        resource_nodes.setdefault(node_name, {}).setdefault(core, []).append(job_data)

        report['method'] = 'from_service_start'

        if all((self.gstats.get('service_start'), self.gstats.get('service_finish'))):
            min_start_moment = self.gstats.get('service_start')
            max_finish_moment = self.gstats.get('service_finish')
        else:
            min_start_moment = self.jstats.get('min_real_start')
            max_finish_moment = self.jstats.get('max_real_finish')
            report['method'] = 'from_first_job_start'

        total_time = (max_finish_moment - min_start_moment).total_seconds()

        total_core_utilization = 0
        total_cores = 0 

        # sort jobs in each of the core by the start time
        for node_name, cores in resource_nodes.items():
            for core_name, core_jobs in cores.items():
                core_jobs.sort(key=lambda job: job['real_start'])

                core_initial_wait = 0
                core_injobs_wait = 0
                core_finish_wait = 0
                core_unused = 0
                if core_jobs:
                    # moment between total scenario start and first job
                    core_initial_wait = (core_jobs[0]['real_start'] - min_start_moment).total_seconds()
                    core_unused += core_initial_wait

                    core_injobs_wait = 0
                    for job_nr in range(1,len(core_jobs)):
                        curr_job = core_jobs[job_nr]
                        prev_job = core_jobs[job_nr-1]

                        # moments between current job start and last job finish
                        core_injobs_wait += (curr_job['real_start'] - prev_job['real_finish']).total_seconds()
                    core_unused += core_injobs_wait

                    # moment between last job finish and total scenario finish
                    core_finish_wait = (max_finish_moment - core_jobs[-1]['real_finish']).total_seconds()
                    core_unused += core_finish_wait

                core_utilization = ((total_time - core_unused) / total_time) * 100
                total_core_utilization += core_utilization
                total_cores += 1

                if details:
                  report.setdefault('nodes', {}).setdefault(node_name, {})[core_name] =  {
                    'unused': core_unused,
                    'utilization': core_utilization,
                    'initial_unused': core_initial_wait,
                    'injobs_unused': core_injobs_wait,
                    'finish_unused': core_finish_wait,
                  }

        report['total_cores'] = total_cores
        report['avg_core_utilization'] = total_core_utilization/total_cores if total_cores else 0

        return report

    def _find_previous_latest_job_finish_on_resources(self, resource_nodes, job_data):
        """On resources allocated for job ``job_data``, find the last, previous job finish.

        Args:
           resource_nodes (dict): a mapping between 'nodes->cores->jobs' where jobs are sorted by the ``real_start`` attribute
           job_data (dict): job attributes (from ``self.jstats['jobs']``)

        Return:
           datetime - a moment when the last job finished, before the specified one, on job's resources.
        """
        max_finish_time = None

        if job_data.get('real_start'):
            job_start = job_data['real_start']
            for node_name, cores in job_data.get('nodes', {}).items():
                # find the last job that finished before 'job_start'
                for core_name in cores:
                    core_jobs = resource_nodes.get(node_name, {}).get(core_name, [])
                    # find first job which start time is >= `job_start`
                    next_job = next((position for position, curr_job_data in enumerate(core_jobs) if curr_job_data['real_start'] >= job_start), None)
                    if next_job is not None and next_job > 0:
                        prev_job = core_jobs[next_job - 1]
                        if max_finish_time is None or prev_job['real_finish'] > max_finish_time:
                            max_finish_time = prev_job['real_finish']
    
        return max_finish_time

    def efficiency(self, details=False):
        resource_nodes = {}
        jobs = {}
        report = {}

        min_start_moment = self.jstats.get('min_real_start')
        max_finish_moment = self.jstats.get('max_real_finish')
        total_time = (max_finish_moment - min_start_moment).total_seconds()

        total_core_utilization = 0
        total_cores = 0 

        # assign jobs to cores
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if job_data.get('nodes') is not None and job_data.get('real_start') is not None:
                for node_name, cores in job_data.get('nodes', {}).items():
                    for core in cores:
                        resource_nodes.setdefault(node_name, {}).setdefault(core, []).append(job_data)

        # sort jobs in each of the core by the start time
        for node_name, cores in resource_nodes.items():
            for core_name, core_jobs in cores.items():
                core_jobs.sort(key=lambda job: job['real_start'])

        # sort jobs in each of the core by the start time
        for node_name, cores in resource_nodes.items():
            for core_name, core_jobs in cores.items():
                core_unused = 0
                prev_job = None

                if core_jobs:
                    for core_job in core_jobs:
                        # moment between total scenario start and first job
                        prev_job_finish_time = self._find_previous_latest_job_finish_on_resources(resource_nodes, core_job)

                        if prev_job_finish_time:
                            core_unused += (core_job['real_start'] - prev_job_finish_time).total_seconds()
                        else:
                            if prev_job:
                                core_unused += (core_job['real_start'] - prev_job['real_finish']).total_seconds()
                            else:
                                core_unused += (core_job['real_start'] - min_start_moment).total_seconds()

                        prev_job = core_job

                core_utilization = ((total_time - core_unused) / total_time) * 100
                if details:
                    report.setdefault('nodes', {}).setdefault(node_name, {})[core_name] = {
                            'unused': core_unused,
                            'utilization': core_utilization
                            }
#                print(f'node {node_name}:{core_name}: utilization {core_utilization:.1f}%, unused {core_unused:.4f} of total {total_time:.4f}')
                total_core_utilization += core_utilization
                total_cores += 1

        report['total_cores'] = total_cores
        report['avg_core_utilization'] = total_core_utilization/total_cores if total_cores else 0
        return report


    def efficiency_core(self, dest_node_name, dest_core_name, details=False):
        resource_nodes = {}
        jobs = {}
        report = {}

        min_start_moment = self.jstats.get('min_real_start')
        max_finish_moment = self.jstats.get('max_real_finish')
        total_time = (max_finish_moment - min_start_moment).total_seconds()

        total_core_utilization = 0
        total_cores = 0 

        # assign jobs to cores
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if job_data.get('nodes') is not None:
                for node_name, cores in job_data.get('nodes', {}).items():
                    for core in cores:
                        resource_nodes.setdefault(node_name, {}).setdefault(core, []).append(job_data)

        # sort jobs in each of the core by the start time
        for node_name, cores in resource_nodes.items():
            for core_name, core_jobs in cores.items():
                core_jobs.sort(key=lambda job: job['real_start'])

#        for job_name, job_data in self.jstats.get('jobs', {}).items():
#            if all(elem in job_data for elem in ['real_start', 'real_finish']):
#                prev_job_finish_time = self._find_previous_latest_job_finish_on_resources(resource_nodes, job_data)
#                print(f'{job_name}: started {job_data.get("real_start")}-{job_data.get("real_finish")} previous latest job finish time {prev_job_finish_time}')

#        print(f'checking core {dest_core_name} on node {dest_node_name} ...')
        core_jobs = resource_nodes.get(dest_node_name, {}).get(dest_core_name, {})
#        print(f'found jobs {",".join([job["name"] for job in core_jobs])}')
        core_unused = 0
        prev_job = None

        if core_jobs:
            for core_job in core_jobs:
#                print(f'checking job {core_job["name"]} ...') 

                # moment between total scenario start and first job
                prev_job_finish_time = self._find_previous_latest_job_finish_on_resources(resource_nodes, core_job)
#                print(f'found previous job latest finish time as {prev_job_finish_time}')

                if prev_job_finish_time:
                    core_unused += (core_job['real_start'] - prev_job_finish_time).total_seconds()
                else:
                    if prev_job:
                        core_unused += (core_job['real_start'] - prev_job['real_finish']).total_seconds()
                    else:
                        core_unused += (core_job['real_start'] - min_start_moment).total_seconds()

                prev_job = core_job
#                print(f'after job {core_job["name"]} core unused {core_unused:.5f} ...') 

        core_utilization = ((total_time - core_unused) / total_time) * 100
        print(f'node {dest_node_name}:{dest_core_name}: utilization {core_utilization:.1f}%, unused {core_unused:.4f} of total {total_time:.4f}')
        total_core_utilization += core_utilization
        total_cores += 1

