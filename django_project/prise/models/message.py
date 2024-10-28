# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message prise models.
"""

from typing import Set
from datetime import datetime, timezone
from django.db import models
from django.db.models import Q
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


class PriseMessageSchedule(models.Model):
    """Class that stores the schedule of sending prise message.

    The scheduler will match by week_of_month and day_of_week first.
    Then, it will also look for matching schedule_date.
    """

    group = models.CharField(
        default=PriseMessageGroup.START_SEASON,
        choices=(
            (
                PriseMessageGroup.START_SEASON,
                _(PriseMessageGroup.START_SEASON)
            ),
            (
                PriseMessageGroup.TIME_TO_ACTION_1,
                _(PriseMessageGroup.TIME_TO_ACTION_1)
            ),
            (
                PriseMessageGroup.TIME_TO_ACTION_2,
                _(PriseMessageGroup.TIME_TO_ACTION_2)
            ),
            (
                PriseMessageGroup.END_SEASON,
                _(PriseMessageGroup.END_SEASON)
            ),
        ),
        max_length=512
    )
    week_of_month = models.PositiveIntegerField(
        blank=True,
        null=True
    )
    day_of_week = models.PositiveIntegerField(
        blank=True,
        null=True
    )
    schedule_date = models.DateField(
        blank=True,
        null=True,
        help_text=(
            'Override the schedule date, '
            'useful for sending one time message.'
        )
    )
    active = models.BooleanField(
        default=True
    )

    @staticmethod
    def calculate_week_of_month(dt: datetime) -> int:
        """Calculate week_of_month from datetime.

        :param dt: datetime object
        :type dt: datetime
        :return: week of month
        :rtype: int
        """
        first_day = dt.replace(day=1)
        day_of_week = first_day.weekday()  # Monday is 0, Sunday is 6
        adjusted_dom = dt.day + day_of_week  # days into the current week
        return int((adjusted_dom - 1) / 7) + 1

    @staticmethod
    def get_schedule(dt: datetime):
        """Get active schedule based on datetime.

        :param dt: datetime object
        :type dt: datetime
        :return: Active schedule of message
        :rtype: List of PriseMessageSchedule
        """
        week_of_month = PriseMessageSchedule.calculate_week_of_month(dt)
        day_of_week = dt.weekday()
        schedule_dt = dt.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        return PriseMessageSchedule.objects.filter(
            active=True
        ).filter(
            Q(
                week_of_month=week_of_month,
                day_of_week=day_of_week
            ) |
            Q(schedule_date=schedule_dt)
        )

    @staticmethod
    def handle_multiple_groups(groups: Set[str]) -> str:
        """Handle when scheduler returns multiple groups.

        :param groups: set of group
        :type groups: Set[str]
        :return: priority group to be sent
        :rtype: str
        """
        if PriseMessageGroup.TIME_TO_ACTION_1 in groups:
            return PriseMessageGroup.TIME_TO_ACTION_1
        if PriseMessageGroup.TIME_TO_ACTION_2 in groups:
            return PriseMessageGroup.TIME_TO_ACTION_2
        return list(groups)[0]
