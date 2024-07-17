# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Reader.
"""

from django.test import TestCase
from datetime import datetime
import xarray as xr
from django.contrib.gis.geos import Point
from unittest.mock import Mock, patch

from core.settings.utils import absolute_path
from gap.utils.reader import DatasetReaderInput
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
        dataset = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        attribute1 = AttributeFactory.create(
            name='Temperature Climatology',
            variable_name='temp_clim')
        dataset_attr1 = DatasetAttributeFactory.create(
            dataset=dataset,
            attribute=attribute1,
            source='temp_clim'
        )
        attribute2 = AttributeFactory.create(
            name='Precipitation Anomaly',
            variable_name='precip_anom')
        dataset_attr2 = DatasetAttributeFactory.create(
            dataset=dataset,
            attribute=attribute2,
            source='precip_anom'
        )
        dt = datetime(2024, 3, 14, 0, 0, 0)
        dt1 = datetime(2024, 3, 15, 0, 0, 0)
        dt2 = datetime(2024, 3, 17, 0, 0, 0)
        p = Point(x=29.12, y=-2.625)
        DataSourceFileFactory.create(
            dataset=dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'salient.nc'
        )
        with patch.object(SalientNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path)
            )
            reader = SalientNetCDFReader(
                dataset, [dataset_attr1, dataset_attr2],
                DatasetReaderInput.from_point(p), dt1, dt2)
            reader.read_forecast_data(dt1, dt2)
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            mock_open.assert_called_once()
            self.assertEqual(len(data_value.results), 3)
            self.assertEqual(
                data_value.results[0].values['temp_clim'], 19.461235)
            self.assertEqual(
                len(data_value.results[0].values['precip_anom']), 50)
