from qcg.pilotjob.executor.job import JobIterations


class Jobs:

    def __init__(self):
        """Store information about currently processed job iterations."""
        self.jobs = dict()

    def add(self, job_iterations):
        """Add job iterations to the database.

        Args:
            job_iterations (JobIterations): job iterations to add
        """
        if job_iterations.id in self.jobs:
            raise KeyError(f'Job {job_iterations.id} already exists')

        self.jobs[job_iterations.id] = job_iterations

    def get(self, job_iterations_id):
        """Return job iterations from the database.

        Args:
            job_iterations_id (str): job iterations identifier

        Returns:
            JobIterations: job iterations

        Raise:
            KeyError: when job iterations with given identifier is not registered in database
        """
        return self.jobs[job_iterations_id]

    def delete(self, job_iterations_id):
        """Remove job iterations from the database.

        Args:
            job_iterations_id (str): job iterations identifier

        Raise:
            KeyError: when job iterations with given identifier is not registered in database
        """
        del self.jobs[job_iterations_id]

