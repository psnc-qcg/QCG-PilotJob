from datetime import datetime
import uuid
import json
import logging
from string import Template

from qcg.pilotjob.common.receiver import RequestHandler
from qcg.pilotjob.common.errors import InvalidRequest, JobAlreadyExist, Unauthorized, JobNotExist, IllegalJobDescription
from qcg.pilotjob.inputqueue.jobdesc import JobDescription, JobResources
from qcg.pilotjob.inputqueue.jobdb import job_db
from qcg.pilotjob.inputqueue.job import Job, JobState
from qcg.pilotjob.common.response import Response
from qcg.pilotjob.inputqueue.executors import IQExecutor


class IQHandler(RequestHandler):
    """InputQueue interface handler"""

    # supported request names
    requests = [
        'submit_job',
        'job_info',
        'list_jobs',
        'list_executors',
        'finish',
        'stats',
        'notify',
        'register_executor',
        'iterations_finished',
        'executor_heartbeat',
        'get_iterations',
    ]

    # prefix of handler functions
    handler_prefix = 'handle_'

    def __init__(self, iqmanager):
        self.iqmanager = iqmanager

    def handle_submit_job(self, request):
        """Handle `submit_job` request.

        Args:
            request (dict): request data, must contain `jobs` key with list of jobs to submit

        Raises:
            InvalidJobRequest - in case of missing some key attributes
            InvalidJobDescription - in case of wrong format of jobs description
        """
        if 'jobs' not in request or not isinstance(request.get('jobs'), list):
            raise InvalidRequest(f'Wrong format of `submit_job` request')

        job_vars = {
            'sdate': str(datetime.now()),
            'jname': None,
            'uniq': None
        }

        new_jobs = {}
        for input_job_desc in request.get('jobs'):
            job_vars['uniq'] = str(uuid.uuid4())

            # default job name
            input_job_desc.setdefault('name', job_vars['uniq'])

            job_vars['jname'] = input_job_desc.get('name')

            # replace variables and parse job description
            try:
                job_desc = JobDescription(**json.loads(Template(json.dumps(input_job_desc)).safe_substitute(job_vars)))
            except Exception as exc:
                raise IllegalJobDescription(f'wrong job description - {str(exc)}')

            # default parameters
            if not job_desc.resources:
                job_desc.resources = JobResources(cores={'exact': 1})

            # validate job description
            job_desc.validate()

            # validate uniqness of job name
            if job_db.exist(job_desc.name) or job_desc.name in new_jobs:
                raise JobAlreadyExist(f'Job {job_desc.name} already exists')

            new_jobs[job_desc.name] = Job(description=job_desc)

        self.iqmanager.enqueue_jobs(new_jobs.values())

        response_data = {
            'submitted': len(new_jobs),
            'jobs': list(new_jobs.keys())
        }

        return Response.ok(f'{len(new_jobs)} submitted', data=response_data)

    def handle_job_info(self, request):
        if 'jobs' not in request or not isinstance(request.get('jobs'), list):
            raise InvalidRequest(f'Wrong format of `job_info` request - missing job names')

        result = {}
        for job_name in request.get('jobs'):
            try:
                job = job_db.get(job_name)
                if not job:
                    raise JobNotExist(job_name)

                result[job_name] = {'status': 'ok', 'state': job.str_state()}
            except JobNotExist:
                result[job_name] = {'status': 'not found'}
            except:
                result[job_name] = {'status': 'error'}

        return Response.ok(f'ok', data={'jobs': result})

    def handle_list_jobs(self, request):
        return Response.error('not supported')

    def handle_list_executors(self, request):
        return Response.error('not supported')

    def handle_finish(self, request):
        return Response.error('not supported')

    def handle_stats(self, request):
        return Response.error('not supported')

    def handle_notify(self, request):
        return Response.error('not supported')

    def handle_register_executor(self, request):
        """Handle `register_executor` request.

        Args:
            request (dict): request data, must contain `executor` key with at least following data:
                name (str): executor name
                checksum (str): encoded executor name
                resources (Resources): executor's available resources

        Raises:
            InvalidRequest - in case of missing some key attributes
            Unauthorized - in case of missing or incorrect checksum
        """
        try:
            if 'executor' not in request or not isinstance(request.get('executor'), dict) or \
                    any(key not in request.get('executor', {}) for key in ['name', 'address', 'checksum', 'resources']):
                raise InvalidRequest(f'Wrong format of `register_executor` request')

            executor_spec = request.get('executor')
            self.authorize_executor(executor_spec)

            self.iqmanager.register_executor(IQExecutor(
                executor_spec.get('name'),
                executor_spec.get('address'),
                executor_spec.get('resources')))

            return Response.ok(f'registered', data=self.iqmanager.manager_info())
        except Exception as exc:
            return Response.error(str(exc))

    def handle_iterations_finished(self, request):
        try:
            self.authorize_executor(request.get('executor'))

            errors = list()
            for notif_job in request.get('jobs'):
                name = notif_job.get('id')

                try:
                    job = job_db.get(name)
                    if not job:
                        raise JobNotExist(name)

                    state_str = notif_job.get('state')
                    if state_str not in JobState.__members__:
                        raise InvalidRequest(f'job state {state_str} not known')
                    state = JobState[state_str]

                    iteration = notif_job.get('iteration')
                    logging.info(f'changing iteration\'s {iteration} of job {name} state to {state}')
                    logging.info(f'job before state change {job.str_state()}')
                    logging.info(f'iteration before state change {job.str_state(iteration)}')
                    job.set_state(state, iteration, notif_job.get('message'))
                    logging.info(f'job after state change {job.str_state()}')
#                    logging.info(f'iteration after state change {job.str_state(iteration)}')
                except Exception as exc:
                    logging.exception('failed to process finished iteration')
                    errors.append(str(exc))

            if errors:
                raise InvalidRequest('; '.join(str(error) for error in errors))

            return Response.ok(f'received')
        except Exception as exc:
            logging.exception('failed to process finished iterations')
            return Response.error(str(exc))

    def handle_executor_heartbeat(self, request):
        try:
            self.authorize_executor(request.get('executor'))
            self.iqmanager.executor_heartbeat(request.get('executor').get('name'))
            return Response.ok(f'received')
        except Exception as exc:
            return Response.error(str(exc))

    def handle_get_iterations(self, request):
        try:
            self.authorize_executor(request.get('executor'))

            allocation = self.iqmanager.executor_reserve_jobs(request.get('executor').get('name'),
                                                              request.get('allocation', {}))
            return Response.ok(f'reserved', data=allocation)
        except Exception as exc:
            logging.exception('failed to return iterations')
            return Response.error(str(exc))

    def validate_checksum(self, checksum, key):
        #TODO
        return True

    def authorize_executor(self, executor_spec):
        if not executor_spec or any(key not in executor_spec for key in ['name', 'checksum']):
            raise InvalidRequest(f'Missing executor data')

        #TODO: validate checksum
        if not self.validate_checksum(executor_spec.get('name'), executor_spec.get('checksum')):
            raise Unauthorized('Checksum incorrect')
