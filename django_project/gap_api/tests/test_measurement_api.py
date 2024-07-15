# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

from datetime import datetime
from typing import List
from django.contrib.gis.geos import Point
from django.urls import reverse
from unittest.mock import patch

from core.tests.common import FakeResolverMatchV1, BaseAPIViewTest
from django_project.gap.models import DatasetAttribute
from django_project.gap.utils.reader import (
    DatasetReaderValue,
    DatasetTimelineValue
)
from gap_api.api_views.measurement import MeasurementAPI
from gap.utils.reader import BaseDatasetReader
from gap.factories import DatasetAttributeFactory


class MockDatasetReader(BaseDatasetReader):
    """Class to mock a dataset reader."""

    def __init__(self, dataset, attributes: List[DatasetAttribute],
                 point: Point, start_date: datetime,
                 end_date: datetime) -> None:
        """Initialize MockDatasetReader class."""
        super().__init__(dataset, attributes, point, start_date, end_date)

    def get_data_values(self) -> DatasetReaderValue:
        """Override data values with a mock object."""
        return DatasetReaderValue(
            {
                'dataset': [self.dataset.name]
            },
            [DatasetTimelineValue(self.start_date, {
                'test': 100
            })]
        )


class CommonMeasurementAPITest(BaseAPIViewTest):
    """Common class for Measurement API Test."""

    def _get_measurement_request(
            self, lat=-2.215, lon=29.125, attributes='max_total_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', providers=None):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_total_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        request_params = (
            f'?lat={lat}&lon={lon}&attributes={attributes}'
            f'&start_date={start_dt}&end_date={end_dt}'
        )
        if providers:
            request_params = request_params + f'&providers={providers}'
        request = self.factory.get(
            reverse('api:v1:get-measurement') + request_params
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        return request


class HistoricalAPITest(CommonMeasurementAPITest):
    """Historical api test case."""

    def test_read_historical_data_empty(self):
        """Test read historical data that returns empty."""
        view = MeasurementAPI.as_view()
        request = self._get_measurement_request()
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {})

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_read_historical_data(self, mocked_reader):
        """Test read historical data."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockDatasetReader
        attribute1 = DatasetAttributeFactory.create()
        attribute2 = DatasetAttributeFactory.create(
            dataset=attribute1.dataset
        )
        attribs = [
            attribute1.attribute.variable_name,
            attribute2.attribute.variable_name
        ]
        request = self._get_measurement_request(
            attributes=','.join(attribs)
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertIn('metadata', response.data)
        self.assertIn('data', response.data)
        response_data = response.data['data']
        self.assertIn(attribute1.dataset.name, response_data)
        results = response_data[attribute1.dataset.name]
        self.assertEqual(len(results), 1)
        self.assertIn('values', results[0])
        self.assertIn('test', results[0]['values'])
        self.assertEqual(100, results[0]['values']['test'])
        # with providers
        request = self._get_measurement_request(
            attributes=','.join(attribs),
            providers='test_empty'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {})


class ForecastAPITest(CommonMeasurementAPITest):
    """Forecast api test case."""

    def test_read_forecast_data_empty(self):
        """Test read forecast data that returns empty."""
        view = MeasurementAPI.as_view()
        request = self._get_measurement_request()
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {})
