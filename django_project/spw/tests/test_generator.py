# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""
from datetime import datetime, timedelta
import pytz
from unittest.mock import patch, MagicMock
from django.test import TestCase
from django.contrib.gis.geos import Point

from gap.models import (
    DatasetAttribute,
    Dataset
)
from gap.utils.reader import (
    DatasetReaderInput,
    DatasetReaderValue,
    DatasetTimelineValue
)
from gap.providers.tio import (
    TomorrowIODatasetReader
)
from spw.generator import (
    SPWOutput,
    _fetch_timelines_data,
    _fetch_ltn_data
)


class TestSPWOutput(TestCase):
    """Unit test for SPWOutput class."""

    def setUp(self):
        """Set the test class."""
        self.point = Point(y=1.0, x=1.0)
        self.input_data = {
            'temperature': [20.5],
            'pressure': [101.3],
            'humidity': 45,
            'metadata': 'some metadata'
        }
        self.expected_data = {
            'temperature': 20.5,
            'pressure': 101.3,
            'humidity': 45
        }

    def test_initialization(self):
        """Test initialization of SPWOutput class."""
        spw_output = SPWOutput(self.point, self.input_data)

        self.assertEqual(spw_output.point, self.point)
        self.assertEqual(
            spw_output.data.temperature, self.expected_data['temperature'])
        self.assertEqual(
            spw_output.data.pressure, self.expected_data['pressure'])
        self.assertEqual(
            spw_output.data.humidity, self.expected_data['humidity'])

    def test_input_data_without_metadata(self):
        """Test initialization of SPWOutput class without metadata."""
        input_data = {
            'temperature': [20.5],
            'pressure': [101.3],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'][0])
        self.assertEqual(spw_output.data.pressure, input_data['pressure'][0])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])

    def test_input_data_with_single_element_list(self):
        """Test initialization of SPWOutput class for single element."""
        input_data = {
            'temperature': [20.5],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'][0])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])

    def test_input_data_with_multiple_element_list(self):
        """Test initialization of SPWOutput class for list."""
        input_data = {
            'temperature': [20.5, 21.0],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])


class TestSPWFetchDataFunctions(TestCase):
    """Test SPW fetch data functions."""

    def setUp(self):
        """Set test fetch data functions."""
        TomorrowIODatasetReader.init_provider()
        self.dataset = Dataset.objects.filter(
            provider__name='Tomorrow.io'
        ).first()
        self.location_input = DatasetReaderInput.from_point(Point(0, 0))
        attr1 = DatasetAttribute.objects.filter(
            source='rainAccumulationSum',
            dataset=self.dataset
        ).first()
        attr2 = DatasetAttribute.objects.filter(
            source='evapotranspirationSum',
            dataset=self.dataset
        ).first()
        self.attrs = [attr1, attr2]
        self.dt_now = datetime.now(tz=pytz.UTC).replace(microsecond=0)
        self.start_dt = self.dt_now - timedelta(days=10)
        self.end_dt = self.dt_now

    @patch.object(TomorrowIODatasetReader, 'read')
    @patch.object(TomorrowIODatasetReader, 'get_data_values')
    def test_fetch_timelines_data(self, mocked_get_data_values, mocked_read):
        """Test fetch timelines data for SPW."""
        mocked_read.side_effect = MagicMock()
        mocked_get_data_values.return_value = (
            DatasetReaderValue(self.location_input.point, [
                DatasetTimelineValue(
                    datetime(2023, 7, 20),
                    {
                        'total_evapotranspiration_flux': 10,
                        'total_rainfall': 5
                    }
                )
            ])
        )
        result = _fetch_timelines_data(
            self.location_input, self.attrs, self.start_dt, self.end_dt)
        expected_result = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5
            }
        }
        self.assertEqual(result, expected_result)


    @patch.object(TomorrowIODatasetReader, 'read')
    @patch.object(TomorrowIODatasetReader, 'get_data_values')
    def test_fetch_ltn_data(self, mocked_get_data_values, mocked_read):
        """Test fetch ltn data for SPW."""
        mocked_read.side_effect = MagicMock()
        mocked_get_data_values.return_value = (
            DatasetReaderValue(self.location_input.point, [
                DatasetTimelineValue(
                    datetime(2023, 7, 20),
                    {'total_evapotranspiration_flux': 8, 'total_rainfall': 3})
            ])
        )
        # Initial historical data
        historical_dict = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5
            }
        }
        result = _fetch_ltn_data(
            self.location_input, self.attrs,
            self.start_dt, self.end_dt, historical_dict)
        expected_result = {
            '07-20': {
                'date': '2023-07-20',
                'evapotranspirationSum': 10,
                'rainAccumulationSum': 5,
                'LTNPET': 8,
                'LTNPrecip': 3
            }
        }
        self.assertEqual(result, expected_result)
