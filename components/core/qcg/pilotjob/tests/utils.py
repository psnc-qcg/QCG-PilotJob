from os import environ, remove
from os.path import dirname, exists, join
import json
import re
import multiprocessing as mp
import queue
import zmq
import time
from datetime import datetime
from subprocess import run, PIPE

from json import JSONDecodeError

import qcg
from qcg.pilotjob.service import QCGPMServiceProcess
from qcg.pilotjob.joblist import JobState
from qcg.pilotjob.utils.auxdir import find_single_aux_dir
from qcg.pilotjob.parseres import get_resources
from qcg.pilotjob.resources import ResourcesType

SHARED_PATH = '/data'


def save_jobs_to_file(jobs, file_path):
    if exists(file_path):
        remove(file_path)

    with open(file_path, 'w') as f:
        f.write(json.dumps([ job.to_dict() for job in jobs ], indent=2))


def save_reqs_to_file(reqs, file_path):
    if exists(file_path):
        remove(file_path)

    with open(file_path, 'w') as f:
        f.write(json.dumps(reqs, indent=2))


def check_job_status_in_json(jobs, workdir='.', dest_state='SUCCEED'):
    to_check = set(jobs)

    entries = {}

    report_path = join(find_single_aux_dir(workdir), 'jobs.report')
    with open(report_path, 'r') as report_f:
        for line, entry in enumerate(report_f, 1):
            try:
                job_entry = json.loads(entry)
            except JSONDecodeError as e:
                raise ValueError('wrong jobs.report {} file format: error in {} line: {}'.format(report_path, line, str(e)))

            for attr in [ 'name', 'state' ]:
                if not attr in job_entry:
                    raise ValueError('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_path, attr))

            jName = job_entry['name']
            state = job_entry['state']

            if jName in to_check:
                msg = job_entry.get('message', None)
                if state != dest_state:
                    raise ValueError('job\'s {} incorrect status \'{}\' vs expected \'{}\' {}'.format(jName, state, dest_state,
                        '(' + msg  + ')' if msg else ''))

                to_check.remove(jName)
                entries[jName] = job_entry

            if len(to_check) == 0:
                break

    if len(to_check) > 0:
        raise ValueError('missing {} jobs in report'.format(','.join(to_check)))

    return entries

def check_service_log_string(resubstr, workdir='.'):
    service_log_file = join(find_single_aux_dir(workdir), 'service.log')

    regex = re.compile(resubstr)

    with open(service_log_file, 'r') as log_f:
        for line in log_f:
            if regex.search(line):
                return True

    return False


def fork_manager(manager_args):
    mp.set_start_method("fork", force=True)

    qcgpm_queue = mp.Queue()
    qcgpm_process = QCGPMServiceProcess(manager_args, qcgpm_queue)
    qcgpm_process.start()

    try:
        qcgpm_conf = qcgpm_queue.get(block=True, timeout=10)
    except queue.Empty:
        raise Exception('Service not started - timeout')
    except Exception as e:
        raise Exception('Service not started: {}'.format(str(e)))

    if not qcgpm_conf.get('zmq_addresses', None):
        raise Exception('Missing QCGPM network interface address')

    return qcgpm_process, qcgpm_conf['zmq_addresses'][0]


def send_request_valid(address, req_data):
    ctx = zmq.Context.instance()
    out_socket = ctx.socket(zmq.REQ)
    out_socket.connect(address)

    try:
        out_socket.send_json(req_data)
        msg = out_socket.recv_json()
        if not msg['code'] == 0:
            raise Exception('request failed: {}'.format(msg.get('message', '')))

        return msg
    finally:
        if out_socket:
            out_socket.close()


def wait_for_job_finish_success(manager_address, jobnames, timeout_secs=0):
    start_time = datetime.now()

    while len(jobnames):
        current_jobnames = []

        status_reply = send_request_valid(manager_address, {'request': 'jobStatus', 'jobNames': jobnames})
        assert all((status_reply['code'] == 0, 'data' in status_reply))
        assert 'jobs' in status_reply['data']
        assert len(status_reply['data']['jobs']) > 0

        for jname in jobnames:
            assert jname in status_reply['data']['jobs']
            jstatus = status_reply['data']['jobs'][jname]
            assert all((jstatus['status'] == 0, jstatus['data']['jobName'] == jname))

            jstate = JobState[jstatus['data']['status']]
            if jstate.is_finished():
                assert jstate == JobState.SUCCEED, str(jstate)
            else:
                current_jobnames.append(jname)

        time.sleep(0.5)
        if timeout_secs and (datetime.now() - start_time).total_seconds() > timeout_secs:
            raise Exception('Timeout occurred')

        jobnames = current_jobnames


def get_allocation_data():
    slurm_job_id = environ.get('SLURM_JOB_ID', None)
    print('slurm job id: {}'.format(slurm_job_id))
    out_p = run(['scontrol', 'show', 'job', slurm_job_id], shell=False, stdout=PIPE, stderr=PIPE)
    out_p.check_returncode()

    raw_data = bytes.decode(out_p.stdout).replace('\n', ' ')
    result = {}
    for element in raw_data.split(' '):
        elements = element.split('=', 1)
        result[elements[0]] = elements[1] if len(elements) > 1 else None

    return result

def set_pythonpath_to_qcg_module():
    # set PYTHONPATH to test sources
    qcg_module_path = dirname((str(qcg.__path__._path[0])))
    print("path to the qcg.pilotjob module: {}".format(qcg_module_path))

    # in case where qcg.pilotjob are not installed in library, we must set PYTHONPATH to run a
    # launcher agnets on other nodes
    if environ.get('PYTHONPATH', None):
        environ['PYTHONPATH'] = ':'.join([environ['PYTHONPATH'], qcg_module_path])
    else:
        environ['PYTHONPATH'] = qcg_module_path


def get_slurm_resources():
    # get # of nodes from allocation
    allocation = get_allocation_data()

    # default config
    config = { }

    resources = get_resources(config)
    assert not resources is None
    assert all((resources.rtype == ResourcesType.SLURM,
                resources.total_nodes == int(allocation.get('NumNodes', '-1')),
                resources.total_cores == int(allocation.get('NumCPUs', '-1')))), \
        'resources: {}, allocation: {}'.format(str(resources), str(allocation))

    assert all((resources.free_cores == resources.total_cores,
                resources.used_cores == 0,
                resources.total_nodes == len(resources.nodes)))

    return resources, allocation

def get_slurm_resources_binded():
    resources, allocation = get_slurm_resources()

    assert resources.binding
    return resources, allocation


def submit_2_manager_and_wait_4_info(manager, jobs, expected_status, **infoKwargs):
    ids = manager.submit(jobs)
    assert len(ids) == len(jobs.job_names())

    manager.wait4all()
    jinfos = manager.info_parsed(ids, **infoKwargs)
    print('parsed job infos: {}'.format(str(jinfos)))

    # check # of jobs is correct
    assert len(jinfos) == len(ids)
    # check expected jobs status
    assert all(jid in jinfos and jinfos[jid].status == expected_status for jid in ids)

    return jinfos
