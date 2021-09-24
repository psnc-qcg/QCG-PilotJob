import datetime
import json
import logging
import uuid

from qcg.pilotjob.errors import InvalidRequest


_logger = logging.getLogger(__name__)


class Request:
    """Base class for all requests.

    Each sub-class should have defined methods:
        to_dict() - serialize request to dictionary
        to_json() - serialize request to JSON format
    and class attribute:
        REQ_NAME - name of the request
    """

    @classmethod
    def parse(cls, data):
        """
        Parse request.

        Args:
            data (dict): parsed data

        Returns:
            req (Request): request object

        Raises:
            InvalidRequest: in case of wrong or unknown request
        """
        if not isinstance(data, dict) or 'request' not in data or not data['request']:
            raise InvalidRequest('Invalid request')

        if data['request'] not in __REQS__:
            raise InvalidRequest('Unknown request name: %s' % data['request'])

        return __REQS__[data['request']](data)


class ControlReq(Request):
    """The control request.

    Attributes:
        command (str): the control command
    """

    REQ_NAME = 'control'

    REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE = 'finishAfterAllTasksDone'
    REQ_CONTROL_CMDS = [
        REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE
    ]

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """
        assert data is not None

        if 'command' not in data or not isinstance(data['command'], str):
            raise InvalidRequest('Wrong control request - missing command')

        if data['command'] not in self.REQ_CONTROL_CMDS:
            raise InvalidRequest('Wrong control request - unknown command "%s"' % (data['command']))

        self.command = data['command']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME, 'command': self.command}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class RegisterReq(Request):
    """The register request.

    Attributes:
        entity (str): the register entity
        params (dict): register parameters, the required keys are ``id``, ``address`` and ``resources``
    """

    REQ_NAME = 'register'

    REQ_REGISTER_ENTITIES = [
        'manager'
    ]

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """
        assert data is not None

        if 'entity' not in data or not data['entity'] in self.REQ_REGISTER_ENTITIES:
            raise InvalidRequest('Wrong register request - missing/unknown entity')

        if 'params' not in data:
            raise InvalidRequest('Wrong register request - missing register parameters')

        if not all(e in data['params'] for e in ['id', 'address', 'resources']):
            raise InvalidRequest('Wrong register request - missing key register parameters')

        self.entity = data['entity']
        self.params = data['params']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME, 'entity': self.entity, 'params': self.params}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class SubmitReq(Request):
    """The submit request.

    Attributes:
        jobs (list(dict)): the list of job descriptions
    """

    REQ_NAME = 'submit'
    REQ_CNT = 1

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data

        Raises:
            InvalidRequest: in case of wrong job description format
        """
        self.jobs = []

        assert data is not None

        if 'jobs' not in data or not data['jobs'] or not isinstance(data['jobs'], list):
            raise InvalidRequest('Wrong submit request - missing jobs data')

        # watch out for values - this data can be copied with the 'shallow' method
        # so complex structures should be omited
        job_vars = {
            'rcnt': str(SubmitReq.REQ_CNT),
            'uniq': str(uuid.uuid4()),
            'sname': 'local',
            'date': str(datetime.datetime.today()),
            'time': str(datetime.time()),
            'dateTime': str(datetime.datetime.now())
        }

        SubmitReq.REQ_CNT += 1

        _logger.debug('request data contains %s jobs', len(data['jobs']))
        new_jobs = []

        for job_desc in data['jobs']:
            if not isinstance(job_desc, dict):
                raise InvalidRequest('Wrong submit request - wrong job data')

            if 'name' not in job_desc:
                raise InvalidRequest('Missing name in job description')

            if 'execution' not in job_desc:
                raise InvalidRequest('Missing execution element in job description')

            # look for 'iterate' directive
            if 'iteration' in job_desc:
                if not isinstance(job_desc['iteration'], dict) or \
                        all(attr not in job_desc['iteration'] for attr in ['stop', 'values']):
                    raise InvalidRequest('Wrong format of iteration directive: not a dictionary')

                if 'stop' in job_desc['iteration']:
                    start = job_desc['iteration'].get('start', 0)
                    end = job_desc['iteration']['stop']
                    if start > end:
                        raise InvalidRequest('Wrong format of iteration directive: start index larger then stop one')

            # default value for missing 'resources' definition
            if 'resources' not in job_desc:
                job_desc['resources'] = {'numCores': {'exact': 1}}

            new_jobs.append({'req': job_desc, 'vars': job_vars.copy()})

        self.jobs.extend(new_jobs)

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        res = {'request': self.REQ_NAME, 'jobs': []}
        for job in self.jobs:
            res['jobs'].append(job['req'])

        return res

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict(), indent=2)


class JobStatusReq(Request):
    """The job status request.

    Attributes:
        job_names (list(str)): the job names list to report status
    """

    REQ_NAME = 'jobStatus'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data

        Raises:
            InvalidRequest: in case of wrong request format
         """
        assert data is not None

        if 'jobNames' not in data or not isinstance(data['jobNames'], list) or len(data['jobNames']) < 1:
            raise InvalidRequest('Wrong job status request - missing job names')

        self.job_names = data['jobNames']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME, 'jobNames': self.job_names}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class JobInfoReq(Request):
    """The job info request.

    Attributes:
        job_names (list(str)): the job names list to report info
        include_childs (bool): does the job's iteration also should be reported
    """

    REQ_NAME = 'jobInfo'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        Raises:
            InvalidRequest: in case of wrong request format
        """
        assert data is not None

        if 'jobNames' not in data or not isinstance(data['jobNames'], list) or len(data['jobNames']) < 1:
            raise InvalidRequest('Wrong job info request - missing job names')

        self.include_childs = False
        if data.get('params'):
            if data['params'].get('withChilds'):
                self.include_childs = True
        self.job_names = data['jobNames']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        result = {'request': self.REQ_NAME, 'jobNames': self.job_names}
        if self.include_childs:
            params = {'withChilds': True}
            result['params'] = params
        return result

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class CancelJobReq(Request):
    """The cancel job request.

    Currently not supported.

    Attributes:
        job_names (list(str)): job names to cancel
    """

    REQ_NAME = 'cancelJob'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """
        assert data is not None

        if 'jobNames' not in data or not isinstance(data['jobNames'], list) or len(data['jobNames']) < 1:
            raise InvalidRequest('Wrong cancel job request - missing job names')

        self.job_names = data['jobNames']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME, 'jobNames': self.job_names}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class RemoveJobReq(Request):
    """Remove job from system.

    Attributes:
        job_names (str): job names to remove
    """
    REQ_NAME = 'removeJob'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """
        assert data is not None

        if 'jobNames' not in data or not isinstance(data['jobNames'], list) or len(data['jobNames']) < 1:
            raise InvalidRequest('Wrong remove job request - missing job names')

        self.job_names = data['jobNames']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME, 'jobNames': self.job_names}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class ListJobsReq(Request):
    """The list jobs request."""
    REQ_NAME = 'listJobs'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class ResourcesInfoReq(Request):
    """The resources info request."""

    REQ_NAME = 'resourcesInfo'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class FinishReq(Request):
    """The finish request."""

    REQ_NAME = 'finish'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class StatusReq(Request):
    """The current statistics request."""

    REQ_NAME = 'status'

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """
        self.allJobsFinished = data.get('options', {}).get('allJobsFinished', False)

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


class NotifyReq(Request):
    """The notify request.

    Attributes:
        entity (str): the notify entity
        params (dict): notify parameters, the required keys are ``name``, ``state`` and ``attributes``
    """

    REQ_NAME = 'notify'

    NOTIFY_ENTITY = [
        'job'
    ]

    def __init__(self, data):
        """Initialize request.

        Args:
            data (dict): request data
        """
        assert data is not None

        if 'entity' not in data or not data['entity'] in self.NOTIFY_ENTITY:
            raise InvalidRequest('Wrong notify request - missing/unknown entity')

        if 'params' not in data:
            raise InvalidRequest('Wrong notify request - missing register parameters')

        if not all(e in data['params'] for e in ['name', 'state', 'attributes']):
            raise InvalidRequest('Wrong notify request - missing key notify parameters')

        self.entity = data['entity']
        self.params = data['params']

    def to_dict(self):
        """Serialize request to dictionary.

        Returns:
            dict: serialized request
        """
        return {'request': self.REQ_NAME, 'entity': self.entity, 'params': self.params}

    def to_json(self):
        """Serialize request to JSON format.

        Returns:
            str: serialized request
        """
        return json.dumps(self.to_dict())


__REQS__ = {
    RegisterReq.REQ_NAME: RegisterReq,
    ControlReq.REQ_NAME: ControlReq,
    SubmitReq.REQ_NAME: SubmitReq,
    JobStatusReq.REQ_NAME: JobStatusReq,
    JobInfoReq.REQ_NAME: JobInfoReq,
    CancelJobReq.REQ_NAME: CancelJobReq,
    RemoveJobReq.REQ_NAME: RemoveJobReq,
    ListJobsReq.REQ_NAME: ListJobsReq,
    ResourcesInfoReq.REQ_NAME: ResourcesInfoReq,
    FinishReq.REQ_NAME: FinishReq,
    StatusReq.REQ_NAME: StatusReq,
    NotifyReq.REQ_NAME: NotifyReq,
}
