# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for CBAM Reader.
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
    CBAMNetCDFReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    NetCDFFileFactory
)


class TestCBAMNetCDFReader(TestCase):
    """Unit test for CBAM NetCDFReader class."""

    @patch('gap.utils.netcdf.daterange_inc',
           return_value=[datetime(2023, 1, 1)])
    @patch('gap.models.NetCDFFile.objects.filter')
    def test_read_historical_data_empty(
        self, mock_filter, mock_daterange_inc):
        """Test for reading historical data that returns empty."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = CBAMNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.first.return_value = None
        reader.read_historical_data(start_date, end_date)
        self.assertEqual(reader.xrDatasets, [])

    def test_read_historical_data(self):
        """Test for reading historical data from CBAM sample."""
        dataset = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        attribute = AttributeFactory.create(
            name='Max Total Temperature',
            variable_name='max_total_temperature')
        dataset_attr = DatasetAttributeFactory.create(
            dataset=dataset,
            attribute=attribute,
            source='max_total_temperature'
        )
        dt = datetime(2019, 11, 1, 0, 0, 0)
        p = Point(x=26.97, y=-12.56)
        NetCDFFileFactory.create(
            dataset=dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'cbam.nc'
        )
        with patch.object(CBAMNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path)
            )
            reader = CBAMNetCDFReader(
                dataset, [dataset_attr], DatasetReaderInput.from_point(p),
                dt, dt)
            reader.read_historical_data(dt, dt)
            mock_open.assert_called_once()
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            self.assertEqual(len(data_value.results), 1)
            self.assertEqual(
                data_value.results[0].values['max_total_temperature'],
                33.371735)
