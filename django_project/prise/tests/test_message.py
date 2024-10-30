# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Message Models.
"""

from datetime import datetime, timedelta
from django.test import TestCase

from gap.factories import PestFactory, FarmGroupFactory
from message.factories import MessageTemplateFactory
from prise.exceptions import PriseMessagePestDoesNotExist
from prise.factories import PriseMessageFactory, PriseMessageScheduleFactory
from prise.models import PriseMessage, PriseMessageSchedule
from prise.variables import PriseMessageGroup


class PriseMessageTest(TestCase):
    """PriseMessage test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set SalientIngestorBaseTest."""
        self.pest_1 = PestFactory(name='pest 1')
        self.pest_2 = PestFactory(name='pest 2')
        self.pest_3 = PestFactory(name='pest 3')
        self.farm_group = FarmGroupFactory()

        message_1 = MessageTemplateFactory(
            template='This is template 1 in english with {{ language_code }}',
            template_sw=(
                'This is template 1 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        message_2 = MessageTemplateFactory(
            template='This is template 2 in english with {{ language_code }}',
            template_sw=(
                'This is template 2 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        message_3 = MessageTemplateFactory(
            template='This is template 3 in english with {{ language_code }}',
            template_sw=(
                'This is template 3 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.TIME_TO_ACTION_1
        )
        message_4 = MessageTemplateFactory(
            template='This is template 4 in english with {{ language_code }}',
            template_sw=(
                'This is template 4 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        message_5 = MessageTemplateFactory(
            template='This is template 5 in english with {{ language_code }}',
            template_sw=(
                'This is template 5 in swahili with {{ language_code }}'
            ),
            group=PriseMessageGroup.START_SEASON
        )
        prise_message = PriseMessageFactory(pest=self.pest_1)
        prise_message.messages.add(*[message_1, message_2, message_3])
        prise_message = PriseMessageFactory(pest=self.pest_2)
        prise_message.messages.add(*[message_4])
        prise_message = PriseMessageFactory(
            pest=self.pest_1, farm_group=self.farm_group
        )
        prise_message.messages.add(*[message_5])

    def test_message_group_not_recognized(self):
        """Test message group is not recognized."""
        with self.assertRaises(ValueError):
            PriseMessage.get_messages_objects(
                self.pest_1, message_group='NotRecognizedGroup'
            )

    def test_pest_no_message_objects(self):
        """Test return pest with no message."""
        with self.assertRaises(PriseMessagePestDoesNotExist):
            PriseMessage.get_messages_objects(self.pest_3)

    def test_pest_1_message_objects(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages_objects(self.pest_1).count(), 3
        )
        self.assertEqual(
            PriseMessage.get_messages_objects(
                self.pest_1, message_group=PriseMessageGroup.START_SEASON
            ).count(),
            2
        )
        self.assertEqual(
            PriseMessage.get_messages_objects(
                self.pest_1, message_group=PriseMessageGroup.TIME_TO_ACTION_1
            ).count(),
            1
        )

    def test_pest_2_message_objects(self):
        """Test return pest 2."""
        self.assertEqual(
            PriseMessage.get_messages_objects(self.pest_2).count(), 1
        )

    def test_pest_1_messages_en_start_season(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 1 in english with en',
                'This is template 2 in english with en',
            ]
        )

    def test_pest_1_messages_sw_start_season(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 1 in swahili with sw',
                'This is template 2 in swahili with sw',
            ]
        )

    def test_pest_1_messages_en_start_season_farm_group_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON,
                farm_group=self.farm_group
            ),
            [
                'This is template 5 in english with en'
            ]
        )

    def test_pest_1_messages_sw_start_season_farm_group_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON,
                farm_group=self.farm_group
            ),
            [
                'This is template 5 in swahili with sw'
            ]
        )

    def test_pest_1_messages_en_time_to_action_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.TIME_TO_ACTION_1
            ),
            [
                'This is template 3 in english with en'
            ]
        )

    def test_pest_1_messages_sw_time_to_action_1(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_1,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.TIME_TO_ACTION_1
            ),
            [
                'This is template 3 in swahili with sw',
            ]
        )

    def test_pest_2_messages_en(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_2,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 4 in english with en',
            ]
        )

    def test_pest_2_messages_sw(self):
        """Test return pest 1."""
        self.assertEqual(
            PriseMessage.get_messages(
                self.pest_2,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON
            ),
            [
                'This is template 4 in swahili with sw',
            ]
        )

    def test_pest_3_messages_en(self):
        """Test return pest 1."""
        with self.assertRaises(PriseMessagePestDoesNotExist):
            PriseMessage.get_messages(
                self.pest_3,
                language_code='en',
                context={
                    'language_code': 'en'
                },
                message_group=PriseMessageGroup.START_SEASON
            )

    def test_pest_3_messages_sw(self):
        """Test return pest 1."""
        with self.assertRaises(PriseMessagePestDoesNotExist):
            PriseMessage.get_messages(
                self.pest_3,
                language_code='sw',
                context={
                    'language_code': 'sw'
                },
                message_group=PriseMessageGroup.START_SEASON
            )


class PriseMessageScheduleTest(TestCase):
    """PriseMessageSchedule test case."""

    def setUp(self):
        """Set test class."""
        # Active schedule matching day_occurrence_in_month and day_of_week
        PriseMessageScheduleFactory.create(
            group="Start Season",
            day_occurrence_in_month=1,
            day_of_week=1,
            schedule_date=None,
            active=True
        )

        # Inactive schedule (should not be returned)
        PriseMessageScheduleFactory.create(
            group="End Season",
            day_occurrence_in_month=1,
            day_of_week=1,
            schedule_date=None,
            active=False
        )

    def test_calculate_day_occurrence_in_month(self):
        """Test calc_day_occurrence_in_month."""
        # Test for the first occurrence of Tuesday (2024-11-05)
        date = datetime(2024, 11, 5)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 1)

        # Test for the second occurrence of Tuesday (2024-11-12)
        date = datetime(2024, 11, 12)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 2)

        # Test for the third occurrence of Tuesday (2024-11-19)
        date = datetime(2024, 11, 19)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 3)

        # Test for the fourth occurrence of Tuesday (2024-11-26)
        date = datetime(2024, 11, 26)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 4)

        # Test for the first occurrence of Monday (2024-11-04)
        date = datetime(2024, 11, 4)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 1)

        # Test the last day of the month (2024-11-30), which is a Saturday
        date = datetime(2024, 11, 30)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 5)

        # Test for the first occurrence of Sunday (2024-12-01)
        date = datetime(2024, 12, 1)
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 1)

        # Test boundary case of the first day of the month
        date = datetime(2024, 11, 1)  # This is a Friday
        self.assertEqual(
            PriseMessageSchedule.calc_day_occurrence_in_month(date), 1)

    def test_get_schedule_by_week_and_day(self):
        """Test get_schedule by week and day."""
        # Test fetching schedule by day_occurrence and day_of_week
        today = datetime(2024, 10, 1)
        schedule = PriseMessageSchedule.get_schedule(today)
        self.assertTrue(schedule)
        self.assertEqual(schedule.group, "Start Season")

    def test_get_schedule_by_schedule_date(self):
        """Test get_schedule by schedule_date."""
        # Test fetching schedule by matching schedule_date
        today = datetime(2024, 10, 28)

        # Active schedule with matching schedule_date
        PriseMessageScheduleFactory.create(
            group="Time to Action",
            day_occurrence_in_month=None,
            day_of_week=None,
            schedule_date=today.date(),
            active=True,
            priority=10
        )

        schedule = PriseMessageSchedule.get_schedule(today)
        self.assertTrue(schedule)
        self.assertEqual(schedule.group, "Time to Action")

    def test_no_schedule_found(self):
        """Test get_schedule where no schedule is found."""
        # Test case where no schedule matches the current date/time
        future_date = datetime(2024, 10, 28) + timedelta(days=30)
        schedule = PriseMessageSchedule.get_schedule(future_date)
        self.assertFalse(schedule)

    def test_inactive_schedule(self):
        """Test get_schedule where there is inactive schedule."""
        # Ensure inactive schedules are not returned
        today = datetime(2024, 10, 28)
        PriseMessageSchedule.objects.create(
            group="Inactive Group",
            day_occurrence_in_month=(
                PriseMessageSchedule.calc_day_occurrence_in_month(today)
            ),
            day_of_week=today.weekday(),
            schedule_date=None,
            active=False
        )

        schedule = PriseMessageSchedule.get_schedule(today)
        self.assertFalse(schedule)
