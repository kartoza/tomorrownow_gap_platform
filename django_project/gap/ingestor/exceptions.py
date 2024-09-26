# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Exceptions for ingestor.
"""


class FileNotFoundException(Exception):
    """File not found."""

    def __init__(self):  # noqa
        self.message = 'File not found.'
        super().__init__(self.message)


class FileIsNotCorrectException(Exception):
    """File not found."""

    def __init__(self, message):  # noqa
        super().__init__(message)


class ApiKeyNotFoundException(Exception):
    """Api key not found."""

    def __init__(self):  # noqa
        self.message = 'Api key not found.'
        super().__init__(self.message)


class EnvIsNotSetException(Exception):
    """Env is not set error."""

    def __init__(self, ENV_KEY):  # noqa
        self.message = f'{ENV_KEY} is not set.'
        super().__init__(self.message)
