from qcg.pilotjob.inputqueue.jobdesc import JobDescription
from qcg.pilotjob.inputqueue.job import JobState

import uuid


class JobIterations:

    def __init__(self, name, description, attributes, iterations):
        """Job to execute.

        Args:
            name (str): job name (reported to the input queue)
            description (JobDescription): job description
            attributes (dict): attributes set by input queue
            iterations (iterable(str,int)): iterations to execute
        """
        self.id = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.attributes = attributes

        self.iterations = iterations

        self.states = {iteration: JobState.QUEUED for iteration in iterations}
        self.not_finished = len(iterations)

    def set_state(self, iteration, state):
        """Set job iteration state.

        Args:
            iteration (str,int): job iteration index
            state (JobState): new job state
        """
        self.states[iteration] = state

        if state.is_finished():
            self.not_finished -= 1

    def get_state(self, iteration):
        """Get job iteration state.

        Args:
            iteration (str,int): job iteration index

        Returns:
            JobState: current iteration status
        """
        return self.states[iteration]

    def all_finished(self):
        """Check if all iterations finished.

        Returns:
            bool: True if all iterations finished
        """
