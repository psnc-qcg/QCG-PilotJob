class AppSchedulerError(Exception):
    pass


class SlurmEnvError(AppSchedulerError):
    pass


class InvalidArgument(AppSchedulerError):
    pass


class InvalidAllocation(AppSchedulerError):
    pass


class InvalidResourceSpec(AppSchedulerError):
    pass


class NotSufficientResources(AppSchedulerError):
    pass


class JobAlreadyExist(AppSchedulerError):
    pass


class IllegalResourceRequirements(AppSchedulerError):
    pass


class IllegalJobDescription(AppSchedulerError):
    pass


class InternalError(AppSchedulerError):
    pass


class JobFileNotExist(AppSchedulerError):
    pass


class InvalidRequest(AppSchedulerError):
    pass


class GovernorConnectionError(AppSchedulerError):
    pass


class ResumeError(AppSchedulerError):
    pass