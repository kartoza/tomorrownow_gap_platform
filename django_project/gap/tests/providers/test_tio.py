# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tomorrow.io Dataset Reader.
"""

from django.test import TestCase
from unittest.mock import patch
from datetime import datetime
import pytz
import requests_mock
from django.contrib.gis.geos import Point

from gap.models import (
    Dataset,
    DatasetAttribute
)
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.providers.tio import TomorrowIODatasetReader


class TestTomorrowIODatasetReader(TestCase):
    """Test class for Tomorrow io dataset reader."""

    def setUp(self):
        """Set test class."""
        TomorrowIODatasetReader.init_provider()
        self.dataset = Dataset.objects.filter(
            provider__name='Tomorrow.io'
        ).first()

        attr = DatasetAttribute.objects.filter(
            source='rainAccumulationSum',
            dataset=self.dataset
        ).first()

        self.attributes = [attr]
        self.location_input = DatasetReaderInput.from_point(
            Point(y=40.7128, x=-74.0060))
        self.start_date = datetime(2023, 7, 1, tzinfo=pytz.UTC)
        self.end_date = datetime(2023, 7, 10, tzinfo=pytz.UTC)

        self.reader = TomorrowIODatasetReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @requests_mock.Mocker()
    @patch('os.environ.get', return_value='dummy_api_key')
    def test_read_historical_data(self, mock_request, mock_env):
        """Test read historical data."""
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startTime': '2023-07-01T00:00:00Z',
                            'values': {
                                'rainAccumulationSum': 5.0
                            }
                        }
                    ]
                }]
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/historical?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader.read_historical_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 1)
        self.assertEqual(self.reader.results[0].values['total_rainfall'], 5.0)

    @requests_mock.Mocker()
    @patch('os.environ.get', return_value='dummy_api_key')
    def test_read_forecast_data(self, mock_request, mock_env):
        """Test read forecast data."""
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startTime': '2023-07-01T00:00:00Z',
                            'values': {
                                'rainAccumulationSum': 10.0
                            }
                        }
                    ]
                }]
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/timelines?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader.read_forecast_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 1)
        self.assertEqual(
            self.reader.results[0].values['total_rainfall'], 10.0)

    @requests_mock.Mocker()
    @patch('os.environ.get', return_value='dummy_api_key')
    def test_read_ltn_data(self, mock_request, mock_env):
        """Test read LTN data."""
        self.reader.dataset = Dataset.objects.filter(
            provider__name='Tomorrow.io',
            type__name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
        ).first()
        mock_response = {
            'data': {
                'timelines': [{
                    'intervals': [
                        {
                            'startDate': '07-01',
                            'values': {
                                'rainAccumulationSum': 2.0
                            }
                        }
                    ]
                }]
            }
        }
        mock_request.post(
            'https://api.tomorrow.io/v4/historical/'
            'normals?apikey=dummy_api_key',
            json=mock_response, status_code=200
        )

        self.reader._read_ltn_data(self.start_date, self.end_date)
        self.assertEqual(len(self.reader.results), 1)
        self.assertEqual(self.reader.results[0].values['total_rainfall'], 2.0)
