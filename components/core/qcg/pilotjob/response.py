import json
from enum import IntEnum


class ResponseCode(IntEnum):
    """Response status."""
    OK = 0
    ERROR = 1


class Response:
    """Response data.

    The response is sent back to the sender of request.

    Attributes:
        code (ResponseCode): response status
        msg (str): response message
        data (*): response data
    """

    def __init__(self, code=ResponseCode.OK, msg=None, data=None):
        """Initialize response.

        Args:
            code (ResponseCode): response status
            msg (str, optional): message
            data (*): data
        """
        self.code = code
        self.msg = msg
        self.data = data

    @classmethod
    def ok(cls, msg=None, data=None):
        """Create success response.

        Args:
            msg (str, optional): message
            data (*, optional): data
        """
        return Response(ResponseCode.OK, msg, data)

    @classmethod
    def error(cls, msg=None, data=None):
        """Create error response.

        Args:
            msg (str, optional): message
            data (*, optional): data
        """
        return Response(ResponseCode.ERROR, msg, data)

    def to_dict(self):
        """Serialize response to dictionary.

        Returns:
            dict: serialized response
        """
        res = {'code': int(self.code)}

        if self.msg is not None:
            res['message'] = self.msg

        if self.data is not None:
            res['data'] = self.data

        return res

    def to_json(self):
        """Serialize response to JSON format.

        Returns:
            str: serialized response
        """
        return json.dumps(self.to_dict(), indent=2)
