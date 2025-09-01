import inspect
from binascii import crc32

import aiobotocore.endpoint
import aiobotocore.retryhandler
from aiobotocore.endpoint import HttpxStreamingBody, StreamingBody  # type: ignore
from aiobotocore.retryhandler import ChecksumError, logger  # type: ignore

try:
    import httpx
except ImportError:
    httpx = None


async def _fixed_check_response(self, attempt_number, response):
    http_response = response[0]
    expected_crc = http_response.headers.get(self._header_name)
    if expected_crc is None:
        logger.debug(
            "crc32 check skipped, the %s header is not in the http response.",
            self._header_name,
        )
    else:
        if inspect.isawaitable(http_response.content):
            data_buf = await http_response.content
        else:
            data_buf = http_response.content

        actual_crc32 = crc32(data_buf) & 0xFFFFFFFF
        if not actual_crc32 == int(expected_crc):
            logger.debug(
                "retry needed: crc32 check failed, expected != actual: %s != %s",
                int(expected_crc),
                actual_crc32,
            )
            raise ChecksumError(
                checksum_type="crc32",
                expected_checksum=int(expected_crc),
                actual_checksum=actual_crc32,
            )


async def convert_to_response_dict(http_response, operation_model):
    """Convert an HTTP response object to a request dict.

    This converts the HTTP response object to a dictionary.

    :type http_response: botocore.awsrequest.AWSResponse
    :param http_response: The HTTP response from an AWS service request.

    :rtype: dict
    :return: A response dictionary which will contain the following keys:
        * headers (dict)
        * status_code (int)
        * body (string or file-like object)

    """
    response_dict = {
        "headers": http_response.headers,
        "status_code": http_response.status_code,
        "context": {
            "operation_name": operation_model.name,
        },
    }
    if response_dict["status_code"] >= 300:
        if inspect.isawaitable(http_response.content):
            response_dict["body"] = await http_response.content
        else:
            response_dict["body"] = http_response.content
    elif operation_model.has_event_stream_output:
        response_dict["body"] = http_response.raw
    elif operation_model.has_streaming_output:
        if httpx and isinstance(http_response.raw, httpx.Response):
            response_dict["body"] = HttpxStreamingBody(http_response.raw)
        else:
            length = response_dict["headers"].get("content-length")
            response_dict["body"] = StreamingBody(http_response.raw, length)
    else:
        if inspect.isawaitable(http_response.content):
            response_dict["body"] = await http_response.content
        else:
            response_dict["body"] = http_response.content
    return response_dict


aiobotocore.retryhandler.AioCRC32Checker._check_response = _fixed_check_response  # type: ignore
aiobotocore.endpoint.convert_to_response_dict = convert_to_response_dict
