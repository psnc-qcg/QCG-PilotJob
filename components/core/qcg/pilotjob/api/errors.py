class QCGPJMAError(Exception):
    pass


class InternalError(QCGPJMAError):
    pass


class InvalidJobDescriptionError(QCGPJMAError):
    pass


class JobNotDefinedError(QCGPJMAError):
    pass


class ConnectionError(QCGPJMAError):
    pass


class WrongArgumentsError(QCGPJMAError):
    pass


class FileError(QCGPJMAError):
    pass


class ServiceError(QCGPJMAError):
    pass
