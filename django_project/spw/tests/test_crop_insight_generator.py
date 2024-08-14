# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""

import csv
from datetime import date, timedelta
from unittest.mock import patch

from django.test import TestCase

from core.factories import UserF
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
        '1.spw_output.json'
    ]

    def setUp(self):
        """Set the test class."""
        self.farm = FarmFactory.create()
        self.farm_2 = FarmFactory.create()
        self.r_model = RModelFactory.create(name='test')
        self.today = date.today()
        self.superuser = UserF.create(
            is_staff=True,
            is_superuser=True,
            is_active=True
        )

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
                output, _date, evap, rain, max, min
        ):
            """Create time data."""
            if last_day == _date:
                return

            output[_date.strftime('%m-%d')] = {
                'date': _date.strftime('%Y-%m-%d'),
                'evapotranspirationSum': evap,
                'rainAccumulationSum': rain,
                'temperatureMax': max,
                'temperatureMin': min
            }
            create_timeline_data(
                output, _date + timedelta(days=1), evap, rain, max, min
            )

        mock_fetch_ltn_data.return_value = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'LTNPET': 8,
                'LTNPrecip': 3
            }
        }

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
            10, 5, 100, 0
        )
        mock_fetch_timelines_data.return_value = fetch_timelines_data_val
        generator = CropInsightFarmGenerator(self.farm)
        generator.generate_spw()
        last_spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm
        ).last()
        forecast = self.farm.farmshorttermforecast_set.all().first()
        self.assertEqual(last_spw.signal, 'Plant NOW Tier 1b')
        self.assertEqual(self.farm.farmshorttermforecast_set.count(), 1)
        self.assertEqual(
            forecast.farmshorttermforecastdata_set.count(), 36
        )

        # For farm 2
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
            1, 2, 4, 3
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
            forecast.farmshorttermforecastdata_set.count(), 36
        )

        # Crop insight report
        self.request = CropInsightRequestFactory.create()
        self.request.farms.add(self.farm)
        self.request.farms.add(self.farm_2)
        generate_insight_report(self.request.id)
        self.request.refresh_from_db()
        with self.request.file.open(mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            idx = 0
            for row in csv_reader:
                idx += 1
                if idx == 3:
                    # Farm Unique ID
                    self.assertEqual(row[0], self.farm.unique_id)
                    # Phone Number
                    self.assertEqual(row[1], self.farm.phone_number)
                    self.assertEqual(row[2], '0.0')  # Latitude
                    self.assertEqual(row[3], '0.0')  # Longitude
                    self.assertEqual(row[4], 'Plant Now')
                    self.assertEqual(
                        row[5],
                        'Both current forecast '
                        'historical rains have good signal to plant.'
                    )
                    self.assertEqual(row[6], '0.0')  # Temp (min)
                    self.assertEqual(row[7], '100.0')  # Temp (max)
                    self.assertEqual(row[8], '5.0')  # Precip (daily)
                if idx == 4:
                    # Farm Unique ID
                    self.assertEqual(row[0], self.farm_2.unique_id)
                    # Phone Number
                    self.assertEqual(row[1], self.farm_2.phone_number)
                    self.assertEqual(row[2], '0.0')  # Latitude
                    self.assertEqual(row[3], '0.0')  # Longitude
                    self.assertEqual(row[4], 'DO NOT PLANT')
                    self.assertEqual(
                        row[5], 'Wait for more positive forecast.'
                    )
                    self.assertEqual(row[6], '3.0')  # Temp (min)
                    self.assertEqual(row[7], '4.0')  # Temp (max)
                    self.assertEqual(row[8], '2.0')  # Precip (daily)

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
