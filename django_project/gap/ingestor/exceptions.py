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
