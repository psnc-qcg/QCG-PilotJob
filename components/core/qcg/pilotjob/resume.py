from qcg.pilotjob.joblist import JobState
from qcg.pilotjob.utils.util import Singletone
from qcg.pilotjob.errors import ResumeError
from qcg.pilotjob.joblist import JobList, Job
from os.path import join, isfile, exists
import json
import logging
import os
import glob


_logger = logging.getLogger(__name__)


class StateTracker():
    """
    This class traces incoming requests and job status changes to record current state which can be used
    to resume prematurely interrupted execution of QCG-Pilot job service.
    """

    def __init__(self, path):
        """Initialize state tracker.

        Attributes:
            reqs_file (str) - path to the file with all submit requests
            finished_file (str) - path to the file with states of all already finished iterations & jobs

        Arguments:
            * path - path to the directory where tracker files will be saved, if None, the current working directory
              will be used
        """
        if path is None or not exists(path):
            raise ResumeError(f'Tracker directory {path} not valid - not defined or not exist')

        self.reqs_file = join(path, f'track.reqs')
        self.finished_file = join(path, f'track.states')

        _logger.debug(f'tracer - status file set to {self.reqs_file}, {self.finished_file}')

    @staticmethod
    def resume(path, manager, progress=False):
        """Resume interrupted task iterations execution.

        Arguments:
            * path (str) - path to the directory where tracker files of interrupted execution has been saved
            * manager (Manager) - a manager class that operate scheduler queue
        """
        if progress:
            print(f'resuming computations with QCG-PilotJob auxiliary directory from {path} ...')

        track_reqs_file = join(path, 'track.reqs')
        if not exists(track_reqs_file) or not isfile(track_reqs_file):
            raise ResumeError(f'tracer - {track_reqs_file} not found or not a file')

        track_states_file = join(path, 'track.states')
        if exists(track_states_file) and not isfile(track_states_file):
            raise ResumeError(f'tracer - {track_states_file} is not a file')

        job_requests = {}
        with open(track_reqs_file, "rt") as reqs_file:
            for req_line in reqs_file:
                job_req = json.loads(req_line)
                jname = job_req['name']
                if jname in job_requests:
                    raise ResumeError(f'tracer - job {jname} contains duplicate entries in {path}')

                job_requests[jname] = Job(**job_req)

        _logger.info(f'tracker - read {len(job_requests)} job submit requests')
        if progress:
            print(f'read {len(job_requests)} previous job descriptions')

        job_statuses = []
        if exists(track_states_file):
            with open(track_states_file, "rt") as states_file:
                for state_line in states_file:
                    job_statuses.append(json.loads(state_line))

        _logger.info(f'read {len(job_statuses)} job states')
        if progress:
            print(f'read {len(job_statuses)} job/iteration previous states')

        jobs_to_enqueue = dict(job_requests)

        for job_status in job_statuses:
            jstate = JobState[job_status['state']]
            jname = job_status['task']
            jit = job_status['iteration']

            if job_status['iteration'] is not None:
                # parse job name to get a real job name
                jname, _ = JobList.parse_jobname(jname)

            if not jname in job_requests:
                raise ResumeError(f'tracer - missing job {jname} submit request in {track_reqs_file}')

            #                if not jstate.is_finished():
            #                    raise ResumeError(f'tracer - job {jname} contains not finished state {jstate}')

            job = job_requests[jname]
            job.set_state(jstate, jit)

            if jit is None:
                # we found job
                if jstate.is_finished():
                    del(jobs_to_enqueue[jname])
            else:
                # we found iteration
                pass

        _logger.info(f'job_requests length {len(job_requests)}')
        _logger.info(f'jobs_to_enqueue length {len(jobs_to_enqueue)}')

        # add all jobs to job list
        for job in job_requests.values():
            manager.job_list.add(job)

        _logger.info(f'tracker - found {len(job_requests)} total jobs with {len(jobs_to_enqueue)} unfinished')

        return jobs_to_enqueue.values()
#        manager.enqueue(jobs_to_enqueue.values())


    def new_submited_jobs(self, jobs):
        """Register new submit job request.
        The new request is appended to all previous requests in the ``self.reqs_file`` file.

        Arguments:
            * jobs (Job[]) - a job list to submit.
        """
        _logger.debug(f'tracer - registering new submit request')
        with open(self.reqs_file, "a") as req_file:
            for job in jobs:
                req_file.write(job.to_json())
                req_file.write("\n")

    def job_finished(self, job, iteration):
        """Register finished job status change.
        In case of final task (iterations) status change (SUCCEED, FAILED, CANCELED, OMITTED) task identifier with
        the final state is saved to the ``self.finished_file`` file.

        Arguments:
            * job (Job) - a job object
            * iteration (str) - task iteration identifier, if None the main job has change state
        """
        jname = job.get_name(iteration)
        jstate = job.str_state(iteration)
        _logger.debug(f'tracer - registering job {jname} finished in state {jstate}')
        with open(self.finished_file, "a") as finished_file:
            finished_file.write(json.dumps({
                'task': jname,
                'iteration': iteration,
                'state': jstate
            }))
            finished_file.write("\n")

    def all_jobs_finished(self):
        """All submited job's finished successfully.
        The tracker files should be removed.
        """
        for filename in [self.reqs_file, self.finished_file]:
            try:
                _logger.info(f'tracer - removing tracker file {filename} ...')
                #os.remove(filename)
            except Exception as exc:
                _logger.warning(f'tracer - failed to remove file {filename}: {str(exc)}')
