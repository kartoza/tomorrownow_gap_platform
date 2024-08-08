# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""

from datetime import date, timedelta
from unittest.mock import patch

from django.test import TestCase

from gap.factories.farm import FarmFactory
from gap.models import Provider, Dataset
from gap.models.crop_insight import (
    FarmSuitablePlantingWindowSignal
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
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set the test class."""
        self.farm = FarmFactory.create()
        self.generator = CropInsightFarmGenerator(self.farm)
        self.r_model = RModelFactory.create(name='test')
        self.today = date.today()

    @patch('spw.generator.main.execute_spw_model')
    @patch('spw.generator.main._fetch_timelines_data')
    @patch('spw.generator.main._fetch_ltn_data')
    def test_spw_generator(
            self, mock_fetch_ltn_data, mock_fetch_timelines_data,
            mock_execute_spw_model
    ):
        """Test calculate_from_point function."""

        fetch_timelines_data_val = {}

        last_day = self.today + timedelta(days=10)

        def create_timeline_data(_date):
            """Create time data."""
            if last_day == _date:
                return

            fetch_timelines_data_val[_date.strftime('%m-%d')] = {
                'date': _date.strftime('%Y-%m-%d'),
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'temperatureMax': 10,
                'temperatureMin': 10
            }
            create_timeline_data(_date + timedelta(days=1))

        create_timeline_data(self.today - timedelta(days=10))

        mock_fetch_timelines_data.return_value = fetch_timelines_data_val
        mock_fetch_ltn_data.return_value = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'LTNPET': 8,
                'LTNPrecip': 3
            }
        }
        r_data = {
            'metadata': {
                'test': 'abcdef'
            },
            'goNoGo': ['Do not plant Tier 1a'],
            'nearDaysLTNPercent': [10.0],
            'nearDaysCurPercent': [60.0],
        }
        mock_execute_spw_model.return_value = (True, r_data)

        self.generator.generate_spw()
        last_spw = FarmSuitablePlantingWindowSignal.objects.last()
        self.assertEqual(last_spw.signal, 'Do not plant Tier 1a')
        self.assertEqual(self.farm.farmshorttermforecast_set.count(), 36)
