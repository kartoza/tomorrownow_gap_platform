# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GAP exceptions.
"""


class FarmGroupIsNotSetException(Exception):
    """Farm group is not set."""

    def __init__(self):  # noqa
        self.message = 'Farm group is not set.'
        super().__init__(self.message)


class FarmWithUniqueIdDoesNotFound(Exception):
    """Exception raised when a unique_id does not found."""

    def __init__(self, unique_id: str):  # noqa
        self.message = (
            f'Farm with unique id {unique_id} does not exist.'
        )
        super().__init__(self.message)
