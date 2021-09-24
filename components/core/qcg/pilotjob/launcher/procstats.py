import os
import json
import logging
import subprocess
import re
import socket
import time
from datetime import datetime

from qcg.pilotjob.utils.processes import update_processes_status

_logger = logging.getLogger(__name__)


class ProcStats:
    """The processes statistics class.

    Attributes:
        proc_stats (dict()) - processes data
        cancel_event (threading.Event) - an event to singal cancel
        nodename (str) - local site name
        interval (float) - the delay between following processes check
    """

    scontrol_split_re = re.compile(r'([\w:\-]+)=(.+?)(?=(\s+[\w:\-]+=)|$)')

    def __init__(self, cancel_event):
        self.proc_stats = dict()
        self.cancel_event = cancel_event
        self.nodename = socket.gethostname()
        self.interval = 1.0

        self.slurm_jobid = os.environ.get('SLURM_JOB_ID')
        _logger.info(f'slurm jobid: {self.slurm_jobid}')

    def launch_slurm_step_info(self):
        try:
            return subprocess.Popen(['scontrol', 'show', '-a', '-d', '-o', 'step', self.slurm_jobid],
                                    stdout=subprocess.PIPE, universal_newlines=True)
        except Exception as exc:
            _logger.warning(f'failed to start step info process: {str(exc)}')

    def process_slurm_step_info(self, step_process):
        steps = dict()

        try:
            stdout_s, stderr_s = step_process.communicate(timeout=3)
            stdout = stdout_s.strip() if stdout_s else stdout_s
            exit_code = step_process.returncode

            if exit_code != 0:
                _logger.warning(f'slurm step info returned with error {exit_code}')
                return steps

            for step_line in stdout.splitlines():
                step_params = dict()
                for params in self.scontrol_split_re.findall(step_line):
                    try:
                        if len(params) < 2:
                            raise Exception(f'{len(params)} params: {str(params)}')

                        (pname, pvalue) = (params[0].strip(), params[1].strip())
                        step_params[pname] = pvalue
                    except Exception as exc:
                        _logger.warning(f'failed to parse step info: {str(exc)}')

                if not 'StepId' in step_params:
                    _logger.warning(f'missing step id in step info line: {step_line}')
                else:
                    steps[step_params['StepId']] = step_params

            _logger.info(f'successfully parsed {len(steps)} step infos')
        except Exception as exc:
            _logger.warning(f'failed to process step info: {str(exc)}')

        return steps

    def merge_step_info(self, steps):
        for step_id, step_data in steps.items():
            #            _logger.info(f'checking step {step_id}: {str(step_data)}')
            if 'SrunHost:Pid' in step_data:
                host_pid = step_data['SrunHost:Pid'].split(':', 1)
                if len(host_pid) > 1:
                    pid = int(host_pid[1])
                    #                    _logger.info(f'found step {step_id} for process {pid}')

                    if pid in self.proc_stats:
                        #                        _logger.info(f'updating process {pid} data with step id {step_id}')
                        self.proc_stats[pid]['slurm_step_id'] = step_id
    #                    else:
    #                        _logger.info(f'not found {pid} in processes stats: {",".join(str(k) for k in self.proc_stats.keys())}')

    def trace(self):
        """Gather information about all processes (whole tree) started by Slurm on this node (the localy started
        processes should be also traced, as they are started by the Agent which is launched by Slurm
        """
        def proc_selector(process):
            """Select processes to be traced.
            Arg:
                process (psutil.Process) - process pid (.pid) and name (.name())
            """
            return process.name() == "slurmstepd"

        try:
            # cyclically gather info about processes
            while not self.cancel_event.is_set():
                try:
                    step_trace_start = time.perf_counter()
                    step_process = None

                    if self.slurm_jobid is not None:
                        step_process = self.launch_slurm_step_info()

                    pid_trace_start = time.perf_counter()
                    update_processes_status(proc_selector, self.proc_stats, self.nodename)
                    pid_trace_secs = time.perf_counter() - pid_trace_start
                    _logger.info(f'gathering processes stats took {pid_trace_secs} secs ...')

                    if step_process:
                        steps = self.process_slurm_step_info(step_process)
                        step_info_trace_secs = time.perf_counter() - step_trace_start
                        _logger.info(f'gathering step info took {step_info_trace_secs} secs ...')
                        merge_step_info_start = time.perf_counter()
                        self.merge_step_info(steps)
                        merge_step_info_secs = time.perf_counter() - merge_step_info_start
                        _logger.info(f'merging step info took {merge_step_info_secs} secs ...')

                    time.sleep(self.interval)
                except Exception as exc:
                    _logger.info(f'something went wrong: {str(exc)}')
                    _logger.exception(exc)
                    raise
        finally:
            _logger.info('finishing gathering processes statistics')
            try:
                trace_fname=f'ptrace_{self.nodename}_{str(datetime.now())}_{str(os.getpid())}.log'

                def set_encoder(obj):
                    if isinstance(obj, set):
                        return list(obj)
                    else:
                        return str(obj)

                with open(trace_fname, 'wt') as out_f:
                    out_f.write(json.dumps({ 'node': self.nodename,
                                             'pids': self.proc_stats },
                                           indent=2, default=set_encoder))
            except Exception as exc:
                _logger.error(f'failed to save process statistics: {str(exc)}')
                _logger.exception(exc)

