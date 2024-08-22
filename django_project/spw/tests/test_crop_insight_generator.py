# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""

import csv
from datetime import date, timedelta
from unittest.mock import patch

from django.contrib.gis.geos import Point
from django.test import TestCase

from core.factories import UserF
from core.group_email_receiver import _group_crop_plan_receiver
from gap.factories.crop_insight import CropInsightRequestFactory
from gap.factories.farm import FarmFactory
from gap.models.crop_insight import (
    FarmSuitablePlantingWindowSignal, CropInsightRequest
)
from gap.tasks.crop_insight import (
    generate_insight_report,
    generate_crop_plan
)
from spw.factories import RModelFactory
from spw.generator.crop_insight import CropInsightFarmGenerator

ltn_returns = {
    '07-20': {
        'date': '2023-07-20',
        'evapotranspirationSum': 10,
        'rainAccumulationSum': 5,
        'LTNPET': 8,
        'LTNPrecip': 3
    }
}


def always_retrying(
        location_input, attrs, start_dt, end_dt, historical_dict: dict
):
    raise Exception('Test exception')


retry = {
    'count': 1
}


def retrying_2_times(
        location_input, attrs, start_dt, end_dt, historical_dict: dict
):
    if retry['count'] <= 2:
        retry['count'] += 1
        raise Exception('Test exception')
    else:
        return ltn_returns


class TestCropInsightGenerator(TestCase):
    """Unit test for Crop Insight Generator class."""

    fixtures = [
        '2.provider.json',
        '3.observation_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '9.rainfall_classification.json',
        '1.spw_output.json'
    ]
    csv_headers = [
        'farmID', 'phoneNumber', 'latitude', 'longitude', 'SPWTopMessage',
        'SPWDescription',
        'day1_mm', 'day1_Chance', 'day1_Type',
        'day2_mm', 'day2_Chance', 'day2_Type',
        'day3_mm', 'day3_Chance', 'day3_Type',
        'day4_mm', 'day4_Chance', 'day4_Type',
        'day5_mm', 'day5_Chance', 'day5_Type',
        'day6_mm', 'day6_Chance', 'day6_Type',
        'day7_mm', 'day7_Chance', 'day7_Type',
        'day8_mm', 'day8_Chance', 'day8_Type',
        'day9_mm', 'day9_Chance', 'day9_Type'
    ]

    def setUp(self):
        """Set the test class."""
        self.farm = FarmFactory.create(
            geometry=Point(11.1111111, 10.111111111)
        )
        self.farm_2 = FarmFactory.create(
            geometry=Point(22.22222222, 100.111111111)
        )
        self.r_model = RModelFactory.create(name='test')
        self.today = date.today()
        self.superuser = UserF.create(
            is_staff=True,
            is_superuser=True,
            is_active=True
        )

        group = _group_crop_plan_receiver()
        self.user_1 = UserF(email='user_1@email.com')
        self.user_2 = UserF(email='user_2@email.com')
        self.user_3 = UserF(email='user_3@email.com')
        group.user_set.add(self.user_1, self.user_2)

    @patch('spw.generator.main.execute_spw_model')
    @patch('spw.generator.main._fetch_timelines_data')
    @patch('spw.generator.main._fetch_ltn_data')
    def test_spw_generator(
            self, mock_fetch_ltn_data, mock_fetch_timelines_data,
            mock_execute_spw_model
    ):
        """Test calculate_from_point function."""
        last_day = self.today + timedelta(days=10)

        def create_timeline_data(
                output, _date, evap, rain, max, min, prec_prob
        ):
            """Create time data."""
            if last_day == _date:
                return

            output[_date.strftime('%m-%d')] = {
                'date': _date.strftime('%Y-%m-%d'),
                'evapotranspirationSum': evap,
                'rainAccumulationSum': rain,
                'temperatureMax': max,
                'precipitationProbability': prec_prob,
                'temperatureMin': min
            }
            create_timeline_data(
                output, _date + timedelta(days=1),
                evap, rain, max, min, prec_prob
            )

        # For farm 1
        mock_execute_spw_model.return_value = (
            True, {
                'metadata': {
                    'test': 'abcdef'
                },
                'goNoGo': ['Plant NOW Tier 1b'],
                'nearDaysLTNPercent': [10.0],
                'nearDaysCurPercent': [60.0],
            }
        )
        fetch_timelines_data_val = {}
        create_timeline_data(
            fetch_timelines_data_val, self.today - timedelta(days=10),
            10, 10, 100, 0, 50
        )
        mock_fetch_timelines_data.return_value = fetch_timelines_data_val
        generator = CropInsightFarmGenerator(self.farm)

        # ------------------------------------------------------------
        # Check if _fetch_ltn_data always returns exception
        with patch('spw.generator.main._fetch_ltn_data', always_retrying):
            with self.assertRaises(Exception):
                generator.generate_spw()

        # ------------------------------------------------------------
        # Check if _fetch_ltn_data always returns exception
        with patch('spw.generator.main._fetch_ltn_data', retrying_2_times):
            generator.generate_spw()
        # ------------------------------------------------------------

        last_spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm
        ).last()
        forecast = self.farm.farmshorttermforecast_set.all().first()
        self.assertEqual(last_spw.signal, 'Plant NOW Tier 1b')
        self.assertEqual(self.farm.farmshorttermforecast_set.count(), 1)
        self.assertEqual(
            forecast.farmshorttermforecastdata_set.count(), 45
        )

        # For farm 2
        mock_fetch_ltn_data.return_value = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'LTNPET': 8,
                'LTNPrecip': 3
            }
        }
        mock_execute_spw_model.return_value = (
            True, {
                'metadata': {
                    'test': 'abcdef'
                },
                'goNoGo': ['Do NOT plant, DRY Tier 4b'],
                'nearDaysLTNPercent': [10.0],
                'nearDaysCurPercent': [60.0],
            }
        )
        fetch_timelines_data_val = {}
        create_timeline_data(
            fetch_timelines_data_val, self.today - timedelta(days=10),
            1, 0.5, 4, 3, 10
        )
        mock_fetch_timelines_data.return_value = fetch_timelines_data_val
        generator = CropInsightFarmGenerator(self.farm_2)
        generator.generate_spw()
        last_spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm_2
        ).last()
        forecast = self.farm_2.farmshorttermforecast_set.all().first()
        self.assertEqual(last_spw.signal, 'Do NOT plant, DRY Tier 4b')
        self.assertEqual(
            self.farm_2.farmshorttermforecast_set.count(), 1
        )
        self.assertEqual(
            forecast.farmshorttermforecastdata_set.count(), 45
        )

        # Crop insight report
        self.request = CropInsightRequestFactory.create()
        self.request.farms.add(self.farm)
        self.request.farms.add(self.farm_2)
        generate_insight_report(self.request.id)
        self.request.refresh_from_db()
        with self.request.file.open(mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            row_num = 1
            for row in csv_reader:
                # Header
                if row_num == 1:
                    self.assertEqual(row, self.csv_headers)

                # First row
                elif row_num == 2:
                    # Farm Unique ID
                    self.assertEqual(row[0], self.farm.unique_id)
                    # Phone Number
                    self.assertEqual(row[1], self.farm.phone_number)
                    self.assertEqual(row[2], '10.1111')  # Latitude
                    self.assertEqual(row[3], '11.1111')  # Longitude
                    self.assertEqual(row[4], 'Plant Now')
                    self.assertEqual(
                        row[5],
                        'Both current forecast '
                        'historical rains have good signal to plant.'
                    )
                    self.assertEqual(row[6], '10.0')  # Precip (daily)
                    self.assertEqual(row[7], '50.0')  # Precip % chance
                    self.assertEqual(row[8], 'Light rain')  # Precip Type

                # Second row
                elif row_num == 3:
                    # Farm Unique ID
                    self.assertEqual(row[0], self.farm_2.unique_id)
                    # Phone Number
                    self.assertEqual(row[1], self.farm_2.phone_number)
                    self.assertEqual(row[2], '100.1111')  # Latitude
                    self.assertEqual(row[3], '22.2222')  # Longitude
                    self.assertEqual(row[4], 'DO NOT PLANT')
                    self.assertEqual(
                        row[5], 'Wait for more positive forecast.'
                    )
                    self.assertEqual(row[6], '0.5')  # Precip (daily)
                    self.assertEqual(row[7], '10.0')  # Precip % chance
                    self.assertEqual(row[8], 'No Rain')  # Precip Type
                row_num += 1

    @patch('spw.generator.crop_insight.CropInsightFarmGenerator.generate_spw')
    @patch('gap.models.crop_insight.CropInsightRequest.generate_report')
    def test_generate_crop_plan(
            self, mock_generate_report, mock_generate_spw
    ):
        """Test generate crop plan for all farms."""
        generate_crop_plan()
        self.assertEqual(
            CropInsightRequest.objects.count(), 1
        )
        self.assertEqual(mock_generate_spw.call_count, 2)
        mock_generate_report.assert_called_once()

    def test_email_send(self):
        """Test email send when report created."""
        self.recipients = []
        parent = self

        def mock_send_fn(self, fail_silently=False):
            """Mock send messages."""
            parent.recipients = self.recipients()
            return 0

        with patch(
                "django.core.mail.EmailMessage.send", mock_send_fn
        ):
            request = CropInsightRequestFactory.create()
            request.generate_report()

            parent.assertEqual(len(self.recipients), 2)
            parent.assertEqual(
                self.recipients,
                [parent.user_1.email, parent.user_2.email]
            )
