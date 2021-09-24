from qcg.pilotjob.api.manager import LocalManager


class QCGPJFuture():
    """QCG-PilotJob Future tracks execution of tasks submitted to QCG-PilotJob via QCGPJExecutor.

    Parameters
    ----------
    ids: list(str)
       list of identifiers of tasks submitted to a QCG-PilotJob manager
    qcgpjm: LocalManager
       QCG-PilotJob manager instance, to which tasks have been submitted

    Returns
    -------
    None

    """

    qcgpjm: LocalManager

    def __init__(self, ids, qcgpjm):
        super().__init__()
        self.ids = ids
        self.qcgpjm = qcgpjm
        self._done = False
        self._cancelled = False

    def result(self, timeout=None):
        """Waits for finish of tasks assigned to this future and once finished results their statuses.

        This method waits until all tasks assigned to the future are executed (successfully or not).
        The QCG-PilotJob manager is periodically polled about status of not finished jobs. The poll interval (2 sec by
        default) can be changed by defining a 'poll_delay' key with appropriate value (in seconds) in
        configuration of instance.

        Parameters
        ----------
        timeout: int
            currently not used

        Returns
        -------
        dict - a map with tasks names and their terminal status
        """
        return self.qcgpjm.wait4(self.ids)

    def done(self):
        """Checks if the future has been finished

        Checks if all tasks assigned to the future are already finished.

        Returns
        -------
        True if all tasks are finished, False otherwise
        """
        if self._done:
            return True

        statuses = self.qcgpjm.status(self.ids)
        for k in statuses['jobs'].keys():
            if not self.qcgpjm.is_status_finished(statuses['jobs'][k]['data']['status']):
                return False

        self._done = True
        return True

    def running(self):
        """Checks if the future is still running

        Checks if any of tasks assigned to the future are still running.

        Returns
        -------
        True if any of tasks is still running, False otherwise
        """
        return not self.done()

    def cancel(self):
        """Cancels the future

        Cancels all tasks assigned to the future.

        Returns
        -------
        True if the operation succeeded.
        """
        if self._cancelled:
            return True

        self.qcgpjm.cancel(self.ids)
        self._cancelled = True
        return True

    def cancelled(self):
        """Checks if the future has been already cancelled

        Checks if the future, and by consequence all the tasks assigned to this future, have been cancelled.

        Returns
        -------
        True if the future has been cancelled, False otherwise.
        """
        return self._cancelled
