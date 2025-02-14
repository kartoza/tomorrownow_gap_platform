# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Prise exceptions.
"""

from prise.variables import PriseDataType


class PriseMessagePestDoesNotExist(Exception):
    """Prise message of pest does not exist exception."""

    def __init__(self, pest):  # noqa
        self.message = (
            f'Prise message with pest {pest.name} does not exist.'
        )
        super().__init__(self.message)


class PestVariableNameNotRecognized(Exception):
    """Exception raised when a variable name is not recognized."""

    def __init__(self, pest_variable_name: str):  # noqa
        self.message = (
            f'Pest variable name {pest_variable_name} is not recognized.'
        )
        super().__init__(self.message)


class PriseDataTypeNotRecognized(Exception):
    """Exception raised when a price data type is not recognized."""

    def __init__(self):  # noqa
        self.message = (
            f'Prise data type is not recognized. '
            f'Choices: {PriseDataType.types()}'
        )
        super().__init__(self.message)
