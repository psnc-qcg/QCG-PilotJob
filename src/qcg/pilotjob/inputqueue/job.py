import logging

from enum import Enum

from qcg.pilotjob.inputqueue.jobdesc import JobDescription
from qcg.pilotjob.common.errors import IllegalJobDescription


class JobState(Enum):
    """The job state."""

    QUEUED = 1
    SCHEDULED = 2
    EXECUTING = 3
    SUCCEED = 4
    FAILED = 5
    CANCELED = 6
    OMITTED = 7

    def is_finished(self):
        """Check if job state is finished (final)."""
        return self in [JobState.SUCCEED, JobState.FAILED, JobState.CANCELED,
                        JobState.OMITTED]

    def is_finished_fail(self):
        """Check if job state is failed finish."""
        return self in [JobState.FAILED, JobState.CANCELED, JobState.OMITTED]


class Job:
    """Job state.

    Attributes:
        description (JobDescription): job description

        attributes (dict): additional attributes
        subjobs (list(JobState)): list of job's iteration states - only if ``iteration`` element defined, elements
            at positions 'job iteration' - 'iteration start'
        subjobs_not_finished (int): number of not finished already iterations - only if ``iteration`` element defined
        subjobs_failed (int): number of already failed iterations - only if ``iteration`` element defined

        job_state (JobState): current state
     """

    def __init__(self, description, attributes=None):
        """Initialize job.

        Args:
            description (JobDescription): job description
            attributes (dict, optional): additional job's attributes used by partition manager

        Raises:
            IllegalJobDescription: raised in case of illegal job name or missing job description
        """
        if not description:
            raise IllegalJobDescription("Missing job description")

        self.description = description
        self.attributes = attributes

        if self.description.iteration:
            self.subjobs = [JobState.QUEUED for i in range(self.description.iteration.iterations())]
            self.subjobs_not_finished = self.description.iteration.iterations()
            self.subjobs_failed = 0

        self.job_state = JobState.QUEUED

    @staticmethod
    def parse_jobname(jobname):
        """Split given name into job name and iteration.

        Args:
            jobname (str): the name to parse

        Returns:
            name, iteration: tuple with job name and iteration, if given job name didn't contain iteration index, the
              second value will be None
        """
        parts = jobname.split(':', 1)
        return parts[0], int(parts[1]) if len(parts) > 1 else None

    def get_name(self, iteration=None):
        """Return job's or job's iteration name.

        Args:
            iteration (int, optional): if defined the iteration's name is returned

        Returns:
            str: job's or job's iteration's name
        """
        return self.description.name if iteration is None else '{}:{}'.format(self.description.name, iteration)

    @property
    def has_iterations(self):
        """bool: true if job has iterations"""
        return self.description.iteration is not None

    @property
    def has_dependencies(self):
        return self.description.dependencies and self.description.dependencies.has_dependencies

    @property
    def iteration(self):
        """JobIteration: ``iteration`` element of job description"""
        return self.description.iteration

    def state(self, iteration=None):
        """Return job's or job's iteration current state.

        Args:
           iteration (int, optional): if defined the iteration's state is returned

        Returns:
           JobState: job's or job's iteration current state
        """
        if self.description.iteration is None and iteration == 0:
            iteration = None

        return self.job_state if iteration is None else self.subjobs[self.iteration.normalize(iteration)]

    def str_state(self, iteration=None):
        """Return job's or job's iteration current state as string.

        Args:
           iteration (int, optional): if defined the iteration's state is returned

        Returns:
           JobState: job's or job's iteration current state as string
        """
        return self.state(iteration).name

    @property
    def iteration_states(self):
        """list(SubJobState): list of iteration states"""
        return self.subjobs

    def set_state(self, state, iteration=None, err_msg=None):
        """Set current job's or job's iteration state.

        Args:
            state (JobState): new job's or job's iteration state
            iteration (int, optional): job's iteration index if the iteration state should be set
            err_msg (str, optional): the optional error message to record

        Returns:
            JobState: the job's finish state if job's iteration status change triggered job status change (for example
                the last iteration job finished, so the whole job also finished), or None if job's as a whole still
                not finished
        """
        logging.debug(f'job {self.get_name()} iteration {iteration} status changed to {state.name}')
        old_state = self.change_state(state, iteration, err_msg)

        if old_state != state:
            if self.description.iteration is not None and iteration is not None:
                if state.is_finished():
                    self.subjobs_not_finished -= 1

                    if state.is_finished_fail():
                        self.subjobs_failed += 1

                    logging.debug(f'currently not finished subjobs {self.subjobs_not_finished}, '
                                  f'failed {self.subjobs_failed}')

                    if self.subjobs_not_finished == 0 and not self.job_state.is_finished():
                        # all subjobs finished - change whole job state
                        final_state = JobState.SUCCEED if self.subjobs_failed == 0 else JobState.FAILED
                        self.change_state(final_state, None)
                        return final_state

        return None

    def change_state(self, state, iteration=None, err_msg=None):
        """Save new status and emit all notifications related to the job (iteration) status change.

        Args:
            state (JobState): a new state
            iteration (int, optional): an iteration number (None if state relates to the main job)
            err_msg (str, optional): an error message (reason) related to the new job status
        """
        logging.debug(f'change state job {self.get_name()} description iteration {self.description.iteration}, argument iteration {iteration}')
        if self.description.iteration is not None and iteration is not None:
            it_idx = self.iteration.normalize(iteration)
            old_state = self.subjobs[it_idx]
            if old_state != state:
                self.subjobs[it_idx] = state
        else:
            old_state = self.job_state
            if old_state != state:
                self.job_state = state

        #TODO: inform all runtime tracking services about job (iteration) status change
        return old_state
