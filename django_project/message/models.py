# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message models.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _

from prise.variables import MessageType


class MessageApplication:
    """The application that will use the message."""

    PRISE = 'PRISE'  # Message that will be used for CABI PRISE


class MessageTemplate(models.Model):
    """Model that stores message template by group and application."""

    code = models.CharField(
        max_length=512, unique=True
    )
    name = models.CharField(
        max_length=512
    )
    type = models.CharField(
        blank=True, null=True, max_length=512
    )
    application = models.CharField(
        default=MessageApplication.PRISE,
        choices=(
            (MessageApplication.PRISE, _(MessageApplication.PRISE)),
        ),
        max_length=512
    )
    group = models.CharField(
        default=MessageType.START_SEASON,
        choices=(
            (MessageType.START_SEASON, _(MessageType.START_SEASON)),
            (MessageType.TIME_TO_ACTION_1, _(MessageType.TIME_TO_ACTION_1)),
            (MessageType.TIME_TO_ACTION_2, _(MessageType.TIME_TO_ACTION_2)),
            (MessageType.END_SEASON, _(MessageType.END_SEASON)),
        ),
        max_length=512
    )
    note = models.TextField(
        blank=True, null=True,
        help_text=_(
            'Just a note about the template.'
        )
    )
    template = models.TextField(
        help_text=_(
            'Field for storing messages in translation. '
            'Include {{ context_key }} as a placeholder '
            'to be replaced with the appropriate context.'
        )
    )

    class Meta:  # noqa
        ordering = ('code',)

    def __str__(self):
        """Return string representation of MessageTemplate."""
        return self.code
