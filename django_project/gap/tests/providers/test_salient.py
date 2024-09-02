# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Reader.
"""

from django.test import TestCase
from datetime import datetime
import xarray as xr
from django.contrib.gis.geos import Point, MultiPoint
from unittest.mock import Mock, patch

from core.settings.utils import absolute_path
from gap.models import DatasetAttribute
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType,
    DatasetReaderValue
)
from gap.utils.netcdf import (
    NetCDFProvider,
)
from gap.providers import (
    SalientNetCDFReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    DataSourceFileFactory
)


class TestSalientNetCDFReader(TestCase):
    """Unit test for Salient NetCDFReader class."""

    def setUp(self):
        """Set Test class for SalientNetCDFReader."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        self.attribute1 = AttributeFactory.create(
            name='Temperature Climatology',
            variable_name='temp_clim')
        self.dataset_attr1 = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute1,
            source='temp_clim'
        )
        self.attribute2 = AttributeFactory.create(
            name='Precipitation Anomaly',
            variable_name='precip_anom')
        self.dataset_attr2 = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute2,
            source='precip_anom'
        )
        self.attributes = [DatasetAttribute(source='var1'),
                           DatasetAttribute(source='var2')]
        self.location_input = DatasetReaderInput.from_point(
            Point(1.0, 2.0)
        )
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = SalientNetCDFReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @patch('gap.models.DataSourceFile.objects.filter')
    @patch('xarray.open_dataset')
    def test_read_forecast_data_empty(self, mock_open_dataset, mock_filter):
        """Test for reading forecast data."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = SalientNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.order_by.return_value.last.return_value = (
            None
        )
        reader.read_forecast_data(start_date, end_date)
        self.assertEqual(reader.xrDatasets, [])

    def test_read_forecast_data(self):
        """Test for reading forecast data from Salient sample."""
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.12, y=-2.625)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            reader = SalientNetCDFReader(
                self.dataset, [self.dataset_attr1, self.dataset_attr2],
                DatasetReaderInput.from_point(p), dt1, dt2)
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values().to_json()
            mock_open.assert_called_once()
            result_data = data_value['data']
            self.assertEqual(len(result_data), 3)
            self.assertAlmostEqual(
                result_data[0]['values']['temp_clim'], 19.461235, 6)
            self.assertEqual(
                len(result_data[0]['values']['precip_anom']), 50)

    def test_read_from_bbox(self):
        """Test for reading forecast data using bbox."""
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.125, y=-2.625)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            reader = SalientNetCDFReader(
                self.dataset, [self.dataset_attr1, self.dataset_attr2],
                DatasetReaderInput.from_bbox(
                    [p.x, p.y, p.x + 0.5, p.y + 0.5]),
                dt1, dt2
            )
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            # temp_clim
            val = dataset['temp_clim'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertAlmostEqual(
                val.values[0], 19.461235, 6)
            # precip_anom
            val = dataset['precip_anom'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertEqual(len(val.values[:, 0]), 50)

    def test_read_from_points(self):
        """Test for reading forecast data using points."""
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.125, y=-2.625)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            location_input = DatasetReaderInput(
                MultiPoint([p, Point(x=p.x + 0.5, y=p.y + 0.5)]),
                LocationInputType.LIST_OF_POINT)
            reader = SalientNetCDFReader(
                self.dataset, [self.dataset_attr1, self.dataset_attr2],
                location_input, dt1, dt2)
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            mock_open.assert_called_once()
            self.assertTrue(isinstance(data_value, DatasetReaderValue))
            self.assertTrue(isinstance(data_value._val, xr.Dataset))
            dataset = data_value.xr_dataset
            # temp_clim
            val = dataset['temp_clim'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertAlmostEqual(
                val.values[0], 19.461235, 6)
            # precip_anom
            val = dataset['precip_anom'].sel(lat=p.y, lon=p.x)
            self.assertEqual(len(val['forecast_day']), 3)
            self.assertEqual(len(val.values[:, 0]), 50)
