import json

import responses


class PatchReqeust:
    """Request object for patch."""

    def __init__(
            self, url: str, response: dict = None, file_response: str = None,
            request_method='GET', status_code=200
    ):
        """Initialize the PatchReqeust.

        :param url: URL to mock
        :param response: Response to be used as responses.
        :param file_response:
            File to be used as responses. It is json file.
            If response is not provided, file_response will be used instead.
        :param request_method: Type request method.
        """
        self.url = url
        self.response = response
        self.file_response = file_response
        self.request_method = request_method
        self.status_code = status_code


class BaseTestWithPatchResponses:
    """Base for test patch with responses."""

    mock_requests = []

    def _mock_request(self, patch_request: PatchReqeust):
        """Mock response with file."""
        request_method = patch_request.request_method
        response = {}
        if patch_request.response:
            response = patch_request.response
        elif patch_request.file_response:
            response = json.loads(
                open(patch_request.file_response, "r").read()
            )

        responses.add(
            responses.GET if request_method == 'GET' else responses.POST,
            patch_request.url,
            status=patch_request.status_code,
            json=response
        )

    def init_mock_requests(self):
        """Init mock requests."""
        for mock_request in self.mock_requests:
            self._mock_request(mock_request)
