# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

import json
from datetime import datetime
from typing import List
from django.urls import reverse
from unittest.mock import patch
from django.contrib.gis.geos import Polygon, MultiPolygon

from core.tests.common import FakeResolverMatchV1, BaseAPIViewTest
from gap.models import DatasetAttribute, Dataset
from gap.utils.reader import (
    DatasetReaderValue,
    DatasetTimelineValue,
    DatasetReaderInput
)
from gap_api.api_views.measurement import MeasurementAPI
from gap.utils.reader import BaseDatasetReader, LocationInputType


class MockDatasetReader(BaseDatasetReader):
    """Class to mock a dataset reader."""

    def __init__(self, dataset, attributes: List[DatasetAttribute],
                 location_input: DatasetReaderInput, start_date: datetime,
                 end_date: datetime) -> None:
        """Initialize MockDatasetReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)

    def get_data_values(self) -> DatasetReaderValue:
        """Override data values with a mock object."""
        if self.location_input.type == LocationInputType.POLYGON:
            p = self.location_input.polygon[0]
        else:
            p = self.location_input.point
        return DatasetReaderValue(
            p,
            [DatasetTimelineValue(self.start_date, {
                'test': 100
            })]
        )


class CommonMeasurementAPITest(BaseAPIViewTest):
    """Common class for Measurement API Test."""

    def _get_measurement_request(
            self, lat=-2.215, lon=29.125, attributes='max_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', product=None):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :param product: product type, defaults to None
        :type product: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        request_params = (
            f'?lat={lat}&lon={lon}&attributes={attributes}'
            f'&start_date={start_dt}&end_date={end_dt}'
        )
        if product:
            request_params = request_params + f'&product={product}'
        request = self.factory.get(
            reverse('api:v1:get-measurement') + request_params
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        return request


    def _post_measurement_request(
            self, lat=-2.215, lon=29.125, attributes='max_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04', product=None):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :param product: product type, defaults to None
        :type product: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        request_params = (
            f'?lat={lat}&lon={lon}&attributes={attributes}'
            f'&start_date={start_dt}&end_date={end_dt}'
        )
        if product:
            request_params = request_params + f'&product={product}'
        polygon = Polygon(((0, 0), (0, 10), (10, 10), (10, 0), (0, 0)))
        data = {
            "type": "FeatureCollection",
            "name": "polygon",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "1"},
                    "geometry": json.loads(MultiPolygon(polygon).json)
                }
            ]
        }
        request = self.factory.post(
            reverse('api:v1:get-measurement') + request_params,
            data=data, format='json'
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        return request


class HistoricalAPITest(CommonMeasurementAPITest):
    """Historical api test case."""

    fixtures = [
        '2.provider.json',
        '3.observation_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def test_read_historical_data_empty(self):
        """Test read historical data that returns empty."""
        view = MeasurementAPI.as_view()
        request = self._get_measurement_request()
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn('metadata', response.data)
        self.assertEqual(response.data['results'], [])

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_read_historical_data(self, mocked_reader):
        """Test read historical data."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockDatasetReader
        dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribute2 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='total_rainfall'
        ).first()
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
        self.assertIn('results', response.data)
        results = response.data['results']
        self.assertEqual(len(results), 1)
        result_data = results[0]['data']
        self.assertIn('values', result_data[0])
        self.assertIn('test', result_data[0]['values'])
        self.assertEqual(100, result_data[0]['values']['test'])
        # with product type
        request = self._get_measurement_request(
            attributes=','.join(attribs),
            product='test_empty'
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn('metadata', response.data)
        self.assertIn('results', response.data)
        self.assertEqual(response.data['results'], [])

    @patch('gap_api.api_views.measurement.get_reader_from_dataset')
    def test_read_historical_data_by_polygon(self, mocked_reader):
        """Test read historical data."""
        view = MeasurementAPI.as_view()
        mocked_reader.return_value = MockDatasetReader
        dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        attribute1 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='max_temperature'
        ).first()
        attribute2 = DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__variable_name='total_rainfall'
        ).first()
        attribs = [
            attribute1.attribute.variable_name,
            attribute2.attribute.variable_name
        ]
        request = self._post_measurement_request(
            attributes=','.join(attribs)
        )
        response = view(request)
        self.assertEqual(response.status_code, 200)
        mocked_reader.assert_called_once_with(attribute1.dataset)
        self.assertIn('metadata', response.data)
        self.assertIn('results', response.data)
        results = response.data['results']
        self.assertEqual(len(results), 1)
