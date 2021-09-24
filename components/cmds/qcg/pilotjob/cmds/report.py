import click
import sys

from qcg.pilotjob.utils.reportstats import JobsReportStats


def job_details_desc(job_data):
    lines = list()

    lines.append(f'\t{job_data.get("name")}:')
    lines.append('\t{:>20}: {}'.format('run-time (s)', str(job_data.get('r_time', 0))))
    lines.append('\t{:>20}: {}'.format('in queue (s)', str(job_data.get('queue_time', 0))))
    lines.append('\t{:>20}: {}'.format('in schedule (s)', str(job_data.get('sched_time', 0))))
    lines.append('\t{:>20}: {}'.format('start (by qcg)', str(job_data.get('s_time', 0))))
    lines.append('\t{:>20}: {}'.format('finish (by qcg)', str(job_data.get('f_time', 0))))
    lines.append('\t{:>20}: {}'.format('start (real)', str(job_data.get('real_start', 0))))
    lines.append('\t{:>20}: {}'.format('finish (real)', str(job_data.get('real_finish', 0))))
    lines.append('\t{:>20}: {}'.format('nodes', str(job_data.get('nodes'))))
    lines.append('\t{:>20}: {}'.format('process id', str(job_data.get('pid'))))
    lines.append('\t{:>20}: {}'.format('process name', str(job_data.get('pname'))))
    if job_data.get('state') != 'SUCCEED':
        lines.append('\t{:>20}: {} ({})'.format('state', str(job_data.get('state')), job_data.get('runtime', {}).get('exit_code', 'UNKNOWN')))
    else:
        lines.append('\t{:>20}: {}'.format('state', str(job_data.get('state'))))

    return '\n'.join(lines)


def job_short_desc(job_data):
    return f'\t{job_data.get("name")} state {job_data.get("state")} run {job_data.get("r_time")} secs, between {job_data.get("real_start")} - {job_data.get("real_finish")} on nodes {",".join(job_data.get("nodes", {}).keys())}'


def print_stats(stats, details=False):
    gstats = stats.global_stats()
    jstats = stats.job_stats()

    if gstats:
        print('\t{:>20}: {}'.format('total jobs', str(gstats.get('total_iterations'))))
        print('\t{:>20}: {}'.format('failed jobs', str(gstats.get('failed_iterations'))))
        print('\t{:>20}: {}'.format('total cores', str(gstats.get('total_cores'))))
        print('\t{:>20}: {}'.format('total nodes', str(gstats.get('total_nodes'))))

        print('\t{:>20}: {}'.format('service started', str(gstats.get('service_start'))))
        print('\t{:>20}: {}'.format('service finished', str(gstats.get('service_finish'))))
        if all((gstats.get('service_start'), gstats.get('service_finish'))):
            service_runtime = f'{(gstats.get("service_finish") - gstats.get("service_start")).total_seconds():.2f}'
        else:
            service_runtime = '???'
        print('\t{:>20}: {} secs'.format('service runtime', service_runtime))

    if jstats and gstats:
        service_start = gstats.get('service_start')
        first_job_start = jstats.get('first_start')

        if all((service_start, first_job_start)):
            init_overhead = f'{(first_job_start - service_start).total_seconds():.2f}'
        else:
            init_overhead = '???'
        print('\t{:>20}: {} secs'.format('init overhead', init_overhead))

        service_finish = gstats.get('service_finish')
        last_job_finish = jstats.get('last_finish')
        if all((service_finish, last_job_finish)):
            finish_overhead = f'{(service_finish - last_job_finish).total_seconds():.2f}'
        else:
            finish_overhead = '???'
        print('\t{:>20}: {} secs'.format('finish overhead', finish_overhead))

        if all((service_start, first_job_start, service_finish, last_job_finish)):
            total_overhead = (first_job_start - service_start).total_seconds() + \
                                (service_finish - last_job_finish).total_seconds()
            service_runtime = (service_finish - service_start).total_seconds()
            overhead_ratio = (total_overhead / service_runtime) * 100.0

            print('\t{:>20}: {:.2f} secs'.format('total overhead', total_overhead))
            print('\t{:>20}: {:.1f} %'.format('overhead ratio', overhead_ratio))

            if gstats.get('total_cores'):
                overhead_core_hours = gstats.get('total_cores') * total_overhead / 3600.0
                print('\t{:>20}: {:.2f}'.format('overhead core-hours', overhead_core_hours))

    if jstats and details:
        print('\t{:>20}: {}'.format('first job queued', str(jstats.get('first_queue', 0))))
        print('\t{:>20}: {}'.format('last job queued', str(jstats.get('last_queue', 0))))
        print('\t{:>20}: {}'.format('total queuing time', jstats['queue_time'].total_seconds() if jstats.get('queue_time') else 0))
        print('\t{:>20}: {}'.format('first job start', str(jstats.get('first_start', 0))))
        print('\t{:>20}: {}'.format('last job finish', str(jstats.get('last_finish', 0))))

        print('\t{:>20}'.format('jobs runtime statistics (seconds):'))
        for k, v in jstats.get('rstats', {}).items():
            print('\t{:>30}: {}'.format(k, v))


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
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def stats(wdir, details, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose)
    print_stats(stats, details)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--verbose', is_flag=True, default=False)
def text(wdir, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose)
    print_text(stats)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('jobids', type=str, nargs=-1)
@click.option('--verbose', is_flag=True, default=False)
def job(wdir, jobids, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose)
    info = stats.job_info(*jobids)
    for job_name, job_data in info.items():
        print(job_details_desc(job_data))


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--state', type=str)
@click.option('--node', type=str)
@click.option('--core', type=str)
@click.option('--sort', type=click.Choice(['start', 'finish','runtime','name','state'], case_sensitive=False))
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def jobs(wdir, state, node, core, sort, details, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose)

    def job_filter(job_data):
        ok = True
        if ok and state and job_data.get('state', '').lower() != state.lower():
#            print(f'searched state {state} does not match job state {job_data.get("state")}')
            ok = False
        if ok and (node or core):
            job_nodes = job_data.get('nodes', {})
            job_node = None

            if job_nodes is None:
                ok = False
            else:
                if node:
                    job_node = job_nodes.get(node, [])
                    if not job_node:
                        ok = False

                if ok and core:
                    if job_node:
                        if core not in job_node:
                            ok = False
                    else:
                        ok = any(core in job_node for _, job_node in job_nodes.items())

        if ok and job_data.get('r_time') is None:
            ok = False

        return ok

    jobs = list(stats.filter_jobs(job_filter))
    if sort:
        if sort == 'start':
            jobs.sort(key=lambda job: job['s_time'])
        elif sort == 'finish':
            jobs.sort(key=lambda job: job['f_time'])
        elif sort == 'runtime':
            jobs.sort(key=lambda job: job['r_time'])
        elif sort == 'name':
            jobs.sort(key=lambda job: job['name'])
        elif sort == 'state':
            jobs.sort(key=lambda job: job['state'])
    for job in jobs:
        if details:
            print(job_details_desc(job))
        else:
            print(job_short_desc(job))


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('node', type=str)
@click.argument('core', type=str)
@click.option('--verbose', is_flag=True, default=False)
def allocation(wdir, node, core, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose)
    jobs = stats.allocation_jobs(node, core)
    print(f'jobs launched on {node}:{core}')
    for job in jobs:
        print(f'\t{job.get("name")} for {job.get("r_time")}, between {job.get("real_start")} - {job.get("real_finish")}')
        print(f'\t\ton nodes {job.get("nodes")}')


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def launch_stats(wdir, details, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose)
    report = stats.job_start_finish_launch_overheads(details=details)
    print('\t{:>40}: {:.4f}'.format('total start overhead', report.get('start', 0)))
    print('\t{:>40}: {:.4f}'.format('total finish overhead', report.get('finish', 0)))
    print('\t{:>40}: {:.4f}'.format('total start and finish overhead', report.get('total', 0)))
    print('\t{:>40}: {:.4f}'.format('average job start overhead', report.get('job_start_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average job finish overhead', report.get('job_finish_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average job total overhead', report.get('job_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average real job run time', report.get('job_real_rt_avg', 0)))
    print('\t{:>40}: {:.4f}'.format('average qcg job run time', report.get('job_qcg_rt_avg', 0)))
    print('\t{:>40}: {:.2f}'.format('average job overhead per runtime (%)', report.get('job_avg_per_rt', 0)))
    print('\t{:>40}: {}'.format('generated for total jobs', report.get('analyzed_jobs', 0)))
    if details and 'jobs' in report:
        print('\t{:>40}'.format('individual job statistics'))
        for job_name, job_overheads in report.get('jobs', {}).items():
            print(f'\t\t{job_name}: total {job_overheads.get("start", 0) + job_overheads.get("finish", 0):.4f}, '
                  f'start {job_overheads.get("start"):.4f}, stop {job_overheads.get("finish"):.4f}')


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--details', is_flag=True, default=False)
@click.option('--wo-init', is_flag=True, default=False)
@click.option('--until-last-job', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def rusage(wdir, details, wo_init, until_last_job, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose=verbose)

    if not stats.has_realtime_stats():
        sys.stderr.write(f'error: real time log files not found - cannot generate statistics')
        sys.exit(1)

    report = stats.resource_usage(from_first_job=wo_init, until_last_job=until_last_job, details=details)

    if report.get('method') != 'from_service_start':
        print('WARNING: due to not sufficient data, resource usage is measured from first job start, NOT from service start')

    if until_last_job:
        print('WARNING: resource usage is counted until last job on given core finished, NOT when allocation finished')

    print('\t{:>40}: {}'.format('used cores', report.get('total_cores', 0)))

    if report.get('not_used_cores', None):
        print(f'WARNING: {report.get("not_used_cores")} has not been used by any job')
        print('\t{:>40}: {:.1f}%'.format('average USED core utilization (%)', report.get('avg_core_utilization', 0)))
        print('\t{:>40}: {:.1f}%'.format('average ALL cores utilization (%)', report.get('avg_all_cores_utilization', 0)))
    else:
        print('\t{:>40}: {:.1f}%'.format('average core utilization (%)', report.get('avg_core_utilization', 0)))


    if details:
        if report.get('nodes'):
         sorted_node_list = sorted(report.get('nodes', {}).keys())
         for node_name in sorted_node_list:
             cores = report['nodes'][node_name]
             print(f'\t{node_name}')
             sorted_core_list = sorted(cores.keys(), key=lambda c: int(c.split('&')[0]) if '&' in str(c) else int(c))
             for core_id in sorted_core_list:
                 core_name = str(core_id)
                 core_spec = cores[core_name]
                 print(f'\t\t{core_name:4}: {core_spec.get("utilization", 0):.1f}%, unused {core_spec.get("unused", 0):.4f} s')

  

@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('output', type=click.Path(file_okay=True, dir_okay=False))
@click.option('--real', is_flag=True, default=True)
@click.option('--verbose', is_flag=True, default=False)
def gantt(wdir, output, real, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose=verbose)

    if not stats.has_realtime_stats():
        real=False
        print(f'warning: real time log files not found - the generated chart might be not accurate')

    try:
        stats.gantt(output, real)
    except ImportError as exc:
        sys.stderr.write(str(exc) + '\n')
        sys.exit(1)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('output', type=click.Path(file_okay=True, dir_okay=False))
@click.option('--real', is_flag=True, default=True)
@click.option('--verbose', is_flag=True, default=False)
def gantt_gaps(wdir, output, real, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose=verbose)

    if not stats.has_realtime_stats():
        real=False
        print(f'warning: real time log files not found - the generated chart might be not accurate')

    try:
        stats.gantt_gaps(output, real)
    except ImportError as exc:
        sys.stderr.write(str(exc) + '\n')
        sys.exit(1)


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def efficiency(wdir, details, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose=verbose)

    if not stats.has_realtime_stats():
        real=False
        print(f'warning: real time log files not found - the generated chart might be not accurate')

    report = stats.efficiency(details)
    print('\t{:>40}: {}'.format('used cores', report.get('total_cores', 0)))
    print('\t{:>40}: {:.1f}%'.format('average core utilization (%)', report.get('avg_core_utilization', 0)))

    if details:
        if report.get('nodes'):
         sorted_node_list = sorted(report.get('nodes', {}).keys())
         for node_name in sorted_node_list:
             cores = report['nodes'][node_name]
             print(f'\t{node_name}')
             sorted_core_list = sorted(cores.keys(), key=lambda c: int(c.split('&')[0]) if '&' in str(c) else int(c))
             for core_id in sorted_core_list:
                 core_name = str(core_id)
                 core_spec = cores[core_name]
                 print(f'\t\t{core_name:4}: {core_spec.get("utilization", 0):.1f}%, unused {core_spec.get("unused", 0):.4f} s')


@reports.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('node', type=str)
@click.argument('core', type=str)
@click.option('--details', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def efficiency_core(wdir, node, core, details, verbose):
    stats = JobsReportStats.from_workdir(wdir, verbose=verbose)

    if not stats.has_realtime_stats():
        real=False
        print(f'warning: real time log files not found - the generated chart might be not accurate')

    stats.efficiency_core(node, core, details)


if __name__ == '__main__':
    reports()
