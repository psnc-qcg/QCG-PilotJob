import json
from datetime import datetime, timedelta


class JobInfo:
    """Object to store parsed job informations.

    Attributes:
        name (str): job name
        status (str): job status
        nodes (dict(str, int[]), optional): dictionary with node names and list of allocated cores
        total_cores (int): number of total allocated cores
        wdir (str): working directory path
        time (timedelta, optional): job run time
        iteration (int, optional): iteration index
        iterations (dict, optional): info about iterations
        childs (JobInfo[], optional): a list of child jobs
        history (str[], optional): list of job status change moments
        messages (str) - messages
    """

    def __init__(self):
        self.name = None
        self.status = None
        self.nodes = None
        self.total_cores = None
        self.wdir = None
        self.time = None
        self.iteration = None
        self.iterations = None
        self.childs = None
        self.history = None
        self.messages = None

    @staticmethod
    def from_child(job_name, child_data):
        """Parse information about a sub job.

        Args:
            job_name (str): job name
            child_data (dict): element of 'childs' from job info response

        Returns:
            JobInfo: instance of job info
        """
        jinfo = JobInfo()
        jinfo.iteration = child_data['iteration']
        jinfo.name = '{}:{}'.format(job_name, jinfo.iteration)
        jinfo.status = child_data['state']

        jinfo.wdir = child_data.get('runtime', {}).get('wd')
        if child_data.get('runtime', {}).get('allocation'):
            jinfo.nodes = JobInfo._parse_allocation(child_data['runtime']['allocation'])
            jinfo.total_cores = sum(len(cores) for _, cores in jinfo.nodes.items())

        if child_data.get('runtime', {}).get('rtime'):
            jinfo.time = JobInfo._parse_runtime(child_data.get('runtime', {}).get('rtime'))

        return jinfo

    @staticmethod
    def from_job(job_data):
        """Parse job info response.

        Args:
            job_data (dict): job information obtained with jobInfo request

        Returns:
            JobInfo: parsed information
        """
        jinfo = JobInfo()

        jinfo.name = job_data.get('jobName')
        jinfo.status = job_data.get('status')

        jinfo.nodes = {}
        jinfo.total_cores = 0
        allocation = job_data.get('runtime', {}).get('allocation')
        if allocation:
            jinfo.nodes = JobInfo._parse_allocation(allocation)
            jinfo.total_cores = sum(len(cores) for _, cores in jinfo.nodes.items())

        jinfo.wdir = job_data.get('runtime', {}).get('wd')
        time = job_data.get('runtime', {}).get('rtime')
        if time:
            jinfo.time = JobInfo._parse_runtime(time)

        jinfo.iterations = job_data.get('iterations', {})

        if 'childs' in job_data:
            jinfo.childs = [JobInfo.from_child(jinfo.name, child_info) for child_info in job_data.get('childs')]

        jinfo.history = [event for event in job_data.get('runtime', {}).get('history', '').splitlines() if event]
        jinfo.messages = job_data.get('messages', None)

        return jinfo

    @staticmethod
    def _parse_allocation(allocation):
        """Parse information about allocation in the form of nodes.

        Args:
            allocation (str): allocation info from qcg-pm service

        Returns:
            dict(str,list(str)): list of node names along with allocated cores on each of the node
        """
        nodes = {}
        if allocation:
            for node in allocation.split(','):
                nodes[node[:node.index('[')]] = node[node.index('[') + 1:-1].split(':')

        return nodes

    @staticmethod
    def _parse_runtime(runtime):
        """Parse timedelta returned by qcg-pm service

        Args:
            runtime (str): timedelta in string format

        Returns:
            timedelta: timedelta representation
        """
        datet = datetime.strptime(runtime, "%H:%M:%S.%f")
        return timedelta(hours=datet.hour, minutes=datet.minute, seconds=datet.second, microseconds=datet.microsecond)

    def __str__(self):
        """Return string representation with all available fields

        Returns:
             str: string representation of job info data
        """
        return json.dumps({attr: str(getattr(self, attr)) for attr in dir(self)
                           if not attr.startswith('_') and not callable(getattr(self, attr))})
