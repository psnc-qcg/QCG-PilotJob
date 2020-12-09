from qcg.pilotjob.errors import JobAlreadyExist
from qcg.pilotjob.inputqueue.job import Job



class JobDb:
    """The list of all submited jobs.

    Attributes:
        _jmap (dict(str,Job)): dictionary with all submited jobs with name as key
    """

    def __init__(self):
        """Initialize the list."""
        self._jmap = {}

    def add(self, job):
        """Add a new job.

        Args:
            job (Job): job to add to the list

        Raises:
            JobAlreadyExist: when job with given name already exists in list
        """
        if self.exist(job.get_name()):
            raise JobAlreadyExist(job.get_name())

        self._jmap[job.get_name()] = job

    def exist(self, jobname):
        """Check if job with given name is in list.

        Args:
            jobname (str): job name to check

        Returns:
            bool: true if job with given name is already in list
        """
        return jobname in self._jmap

    def get(self, jobname):
        """Return job with given name.

        Args:
            jobname (str): job name

        Returns:
            Job: job from the list or None if not such job has been added.
        """
        return self._jmap.get(jobname, None)

    def jobs(self):
        """Return all job names in the list.

        Returns:
            set-like object with job names
        """
        return self._jmap.keys()

    def remove(self, jobname):
        """Remove job with given name from list.

        Args:
            jobname (str): job's name to remove from list
        """
        del self._jmap[jobname]

    def clear(self):
        """Remove all jobs from database.
        For testing purposes.
        """
        self._jmap.clear()


# a global instance of job's list
job_db = JobDb()

