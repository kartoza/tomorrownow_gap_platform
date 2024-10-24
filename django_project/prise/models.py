# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message prise models.
"""

from django.db import models

from gap.models.crop_insight import Pest
from message.models import MessageTemplate


class PriseMessagePestDoesNotExist(Exception):
    """Prise message of pest does not exist exception."""

    def __init__(self, pest: Pest):  # noqa
        self.message = (
            f'Prise message with pest {pest.name} does not exist.'
        )
        super().__init__(self.message)


class PriseMessage(models.Model):
    """Model that stores message template linked with pest."""

    pest = models.ForeignKey(
        Pest, on_delete=models.CASCADE, unique=True
    )
    messages = models.ManyToManyField(
        MessageTemplate, null=True, blank=True
    )

    def __str__(self):
        """Return string representation."""
        return self.pest.name

    class Meta:  # noqa
        ordering = ('pest__name',)
        db_table = 'prise_message'

    @staticmethod
    def get_messages_objects(pest: Pest, message_group: str = None):
        """Return message objects."""
        try:
            message = PriseMessage.objects.get(pest=pest).messages.all()
            if message_group:
                message = message.filter(group=message_group)
            return message
        except PriseMessage.DoesNotExist:
            raise PriseMessagePestDoesNotExist(pest)

    @staticmethod
    def get_messages(
            pest: Pest, message_group: str, context=dict,
            language_code: str = None

    ):
        """Return messages string."""
        return [
            message.get_message(context, language_code)
            for message in PriseMessage.get_messages_objects(
                pest, message_group
            )
        ]
