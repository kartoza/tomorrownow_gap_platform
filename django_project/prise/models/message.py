# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message prise models.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _

from gap.models.farm_group import FarmGroup
from gap.models.pest import Pest
from message.models import MessageTemplate
from prise.exceptions import PriseMessagePestDoesNotExist
from prise.variables import PriseMessageGroup


class PriseMessage(models.Model):
    """Model that stores message template linked with pest."""

    pest = models.ForeignKey(
        Pest, on_delete=models.CASCADE
    )
    farm_group = models.ForeignKey(
        FarmGroup, on_delete=models.CASCADE,
        help_text='A default message if farm group is not specified.',
        null=True, blank=True
    )
    messages = models.ManyToManyField(
        MessageTemplate, blank=True
    )

    def __str__(self):
        """Return string representation."""
        return self.pest.name

    class Meta:  # noqa
        unique_together = ('pest', 'farm_group')
        ordering = ('pest__name',)
        db_table = 'prise_message'
        verbose_name = _('Message')

    @staticmethod
    def get_messages_objects(
            pest: Pest, message_group: str = None, farm_group: FarmGroup = None
    ):
        """Return message objects.

        :param pest: Message for specific pest.
        :type pest: Pest

        :param message_group:
            Message group for specific pest can be checked in
            PriseMessageGroup.
        :type message_group: str

        :param farm_group:
            Message that will be filtered by farm group.
            If not specified, it will use message that belongs to
            empty farm group.
        :type farm_group: FarmGroup
        """
        try:
            message = PriseMessage.objects.get(
                pest=pest, farm_group=farm_group
            ).messages.all()

            if message_group:
                if message_group not in PriseMessageGroup.groups():
                    raise ValueError(
                        'Message group is not recognized. '
                        f'Choices are {PriseMessageGroup.groups()}.'
                    )
                message = message.filter(group=message_group)
            return message
        except PriseMessage.DoesNotExist:
            raise PriseMessagePestDoesNotExist(pest)

    @staticmethod
    def get_messages(
            pest: Pest, message_group: str, context=dict,
            language_code: str = None, farm_group: FarmGroup = None

    ):
        """Return messages string.

        :param pest: Message for specific pest.
        :type pest: Pest

        :param message_group:
            Message group for specific pest can be checked in
            PriseMessageGroup.
        :type message_group: str

        :param context: Context that will be used to render messages.
        :type context: dict

        :param language_code: Language code for messages, default=en.
        :type language_code: str

        :param farm_group:
            Message that will be filtered by farm group.
            If not specified, it will use message that belongs to
            empty farm group.
        :type farm_group: FarmGroup
        """
        return [
            message.get_message(context, language_code)
            for message in PriseMessage.get_messages_objects(
                pest=pest, message_group=message_group, farm_group=farm_group
            )
        ]
