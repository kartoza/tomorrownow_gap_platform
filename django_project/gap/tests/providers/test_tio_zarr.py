# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Reader.
"""

from django.test import TestCase
from datetime import datetime
import xarray as xr
import numpy as np
import pandas as pd
from django.contrib.gis.geos import Point, MultiPoint
from unittest.mock import Mock, patch

from gap.models import (
    DatasetAttribute, Dataset, DatasetStore,
    Attribute
)
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType,
    DatasetReaderValue
)
from gap.providers.tio import (
    TioZarrReaderValue,
    TioZarrReader
)
from gap.factories import (
    DataSourceFileFactory
)
from gap.tests.ingestor.test_tio_shortterm_ingestor import (
    mock_open_zarr_dataset,
    LAT_METADATA,
    LON_METADATA
)


class TestTioZarrReaderValue(TestCase):
    """Unit test for class TioZarrReaderValue."""

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
        """Set TestTioZarrReaderValue class."""
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        # Mocking DatasetAttribute
        self.attribute = DatasetAttribute.objects.filter(
            dataset=self.dataset,
            attribute__variable_name='max_temperature'
        ).first()

        # Creating mock DatasetReaderInput
        point = Point(30, 10, srid=4326)
        self.mock_location_input = DatasetReaderInput.from_point(point)

        # Creating filtered xarray dataset
        forecast_days = np.array([0, 1, 2])
        lats = np.array([10, 20])
        lons = np.array([30, 40])
        temperature_data = np.random.rand(
            len(forecast_days), len(lats), len(lons))

        self.mock_xr_dataset = xr.Dataset(
            {
                "max_temperature": (
                    ["forecast_day_idx", "lat", "lon"], temperature_data
                ),
            },
            coords={
                "forecast_day_idx": forecast_days,
                "lat": lats,
                "lon": lons,
            }
        )
        # Mock forecast_date
        self.forecast_date = np.datetime64('2023-01-01')
        variables = [
            'forecast_day_idx',
            'max_temperature'
        ]
        self.mock_xr_dataset = self.mock_xr_dataset[variables].sel(
            lat=point.y,
            lon=point.x, method='nearest'
        ).where(
            (self.mock_xr_dataset['forecast_day_idx'] >= 0) &
            (self.mock_xr_dataset['forecast_day_idx'] <= 1),
            drop=True)

        # TioZarrReaderValue initialization with xarray dataset
        self.tio_reader_value_xr = TioZarrReaderValue(
            val=self.mock_xr_dataset,
            location_input=self.mock_location_input,
            attributes=[self.attribute],
            forecast_date=self.forecast_date
        )

    def test_initialization(self):
        """Test initialization method."""
        self.assertEqual(
            self.tio_reader_value_xr.forecast_date, self.forecast_date)
        self.assertTrue(self.tio_reader_value_xr._is_xr_dataset)

    def test_post_init(self):
        """Test post initialization method."""
        # Check if the renaming happened correctly
        self.assertIn(
            'forecast_day', self.tio_reader_value_xr.xr_dataset.coords)
        self.assertIn(
            'max_temperature', self.tio_reader_value_xr.xr_dataset.data_vars)
        self.assertNotIn(
            'forecast_day_idx', self.tio_reader_value_xr.xr_dataset.coords
        )

        # Check if forecast_day has been updated to actual dates
        forecast_days = pd.date_range('2023-01-01', periods=2)
        xr_forecast_days = pd.to_datetime(
            self.tio_reader_value_xr.xr_dataset.forecast_day.values)
        pd.testing.assert_index_equal(
            pd.Index(xr_forecast_days), forecast_days)

    def test_is_empty(self):
        """Test is_empty method."""
        self.assertFalse(self.tio_reader_value_xr.is_empty())

    def test_to_json_with_point_type(self):
        """Test convert to_json with point."""
        result = self.tio_reader_value_xr.to_json()
        self.assertIn('geometry', result)
        self.assertIn('data', result)
        self.assertIsInstance(result['data'], list)

    def test_to_json_with_non_point_type(self):
        """Test convert to_json with exception."""
        self.mock_location_input.type = 'polygon'
        with self.assertRaises(TypeError):
            self.tio_reader_value_xr.to_json()

    def test_xr_dataset_to_dict(self):
        """Test convert xarray dataset to dict."""
        result_dict = self.tio_reader_value_xr._xr_dataset_to_dict()
        self.assertIn('geometry', result_dict)
        self.assertIn('data', result_dict)
        self.assertIsInstance(result_dict['data'], list)


class TestTioZarrReader(TestCase):
    """Unit test for Tio Zarr Reader class."""

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
        """Set Test class for Tio Zarr Reader."""
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        self.zarr_source = DataSourceFileFactory.create(
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            name='tio.zarr',
            is_latest=True
        )
        self.attribute1 = Attribute.objects.get(
            name='Max Temperature',
            variable_name='max_temperature')
        self.dataset_attr1 = DatasetAttribute.objects.get(
            dataset=self.dataset,
            attribute=self.attribute1,
            source='max_temperature'
        )
        self.attributes = [self.dataset_attr1]
        self.location_input = DatasetReaderInput.from_point(
            Point(LON_METADATA['min'], LAT_METADATA['min'])
        )
        self.start_date = datetime(2024, 10, 3)
        self.end_date = datetime(2024, 10, 5)
        self.reader = TioZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @patch('gap.models.DataSourceFile.objects.filter')
    def test_read_forecast_data_empty(self, mock_filter):
        """Test for reading forecast data that returns empty."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = TioZarrReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.order_by.return_value.last.return_value = (
            None
        )
        reader.read_forecast_data(start_date, end_date)
        self.assertEqual(reader.xrDatasets, [])

    def test_read_forecast_data(self):
        """Test for reading forecast data."""
        dt1 = datetime(2024, 10, 3)
        dt2 = datetime(2024, 10, 5)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values().to_json()
            mock_open.assert_called_once()
            result_data = data_value['data']
            self.assertEqual(len(result_data), 3)
            self.assertIn('max_temperature', result_data[0]['values'])

    def test_read_from_bbox(self):
        """Test for reading forecast data using bbox."""
        dt1 = datetime(2024, 10, 3)
        dt2 = datetime(2024, 10, 5)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            self.reader.location_input = DatasetReaderInput.from_bbox(
                [
                    LON_METADATA['min'],
                    LAT_METADATA['min'],
                    LON_METADATA['min'] + LON_METADATA['inc'],
                    LAT_METADATA['min'] + LAT_METADATA['inc']
                ]
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('max_temperature', dataset.data_vars)

    def test_read_from_points(self):
        """Test for reading forecast data using points."""
        dt1 = datetime(2024, 10, 3)
        dt2 = datetime(2024, 10, 5)
        with patch.object(self.reader, 'open_dataset') as mock_open:
            mock_open.return_value = mock_open_zarr_dataset()
            p1 = Point(LON_METADATA['min'], LAT_METADATA['min'])
            p2 = Point(
                LON_METADATA['min'] + LON_METADATA['inc'],
                LAT_METADATA['min'] + LAT_METADATA['inc']
            )
            self.reader.location_input = DatasetReaderInput(
                MultiPoint([p1, p2]),
                LocationInputType.LIST_OF_POINT
            )
            self.reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(self.reader.xrDatasets), 1)
            data_value = self.reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            self.assertIn('max_temperature', dataset.data_vars)
