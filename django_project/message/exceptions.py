# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message exceptions.
"""

from django.conf import settings


class MessageLanguageNotSupportedException(Exception):
    """Message language not supported exception."""

    def __init__(self):  # noqa
        self.message = (
            f'The language is not supported. '
            f'Choices: {[lang[0] for lang in settings.LANGUAGES]}'' '
        )
        super().__init__(self.message)
