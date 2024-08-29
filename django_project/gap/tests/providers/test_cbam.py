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
    DataSourceFileFactory
)


class TestCBAMNetCDFReader(TestCase):
    """Unit test for CBAM NetCDFReader class."""

    def setUp(self) -> None:
        """Set test class."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        self.attribute = AttributeFactory.create(
            name='Max Temperature',
            variable_name='max_temperature')
        self.dataset_attr = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute,
            source='max_total_temperature'
        )

    @patch('gap.utils.netcdf.daterange_inc',
           return_value=[datetime(2023, 1, 1)])
    @patch('gap.models.DataSourceFile.objects.filter')
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
        dt = datetime(2019, 11, 1, 0, 0, 0)
        p = Point(x=26.97, y=-12.56)
        DataSourceFileFactory.create(
            dataset=self.dataset,
            start_date_time=dt,
            end_date_time=dt
        )
        file_path = absolute_path(
            'gap', 'tests', 'netcdf', 'cbam.nc'
        )
        with patch.object(CBAMNetCDFReader, 'open_dataset') as mock_open:
            mock_open.return_value = (
                xr.open_dataset(file_path, engine='h5netcdf')
            )
            reader = CBAMNetCDFReader(
                self.dataset, [self.dataset_attr],
                DatasetReaderInput.from_point(p),
                dt, dt)
            reader.read_historical_data(dt, dt)
            mock_open.assert_called_once()
            self.assertEqual(len(reader.xrDatasets), 1)
            data_value = reader.get_data_values()
            self.assertEqual(len(data_value.results), 1)
            self.assertAlmostEqual(
                data_value.results[0].values['max_temperature'],
                33.371735, 6)

    def test_get_data_values_from_multiple_locations(self):
        """Test get data values from several locations."""
        dt = datetime(2019, 11, 1, 0, 0, 0)
        p = Point(x=26.97, y=-12.56)
        attr1 = DatasetAttributeFactory.create(
            dataset=self.dataset,
            source='var1'
        )
        attr2 = DatasetAttributeFactory.create(
            dataset=self.dataset,
            source='var2'
        )
        reader = CBAMNetCDFReader(
            self.dataset, [attr1, attr2],
            DatasetReaderInput.from_point(p),
            dt, dt)
        date_variable = 'time'
        date_values = [datetime(2020, 1, 1), datetime(2020, 1, 2)]
        data_var1 = [[[10, 20], [30, 40]], [[50, 60], [70, 80]]]
        data_var2 = [[[90, 100], [110, 120]], [[130, 140], [150, 160]]]

        val = xr.Dataset(
            {
                'var1': (['time', 'lat', 'lon'], data_var1),
                'var2': (['time', 'lat', 'lon'], data_var2)
            },
            coords={'time': date_values, 'lat': [0, 1], 'lon': [0, 1]}
        )

        locations = [Point(0, 0), Point(1, 1), Point(0, 1), Point(1, 0)]

        reader.date_variable = date_variable
        result = reader._get_data_values_from_multiple_locations(
            val, locations, 2, 2)
        self.assertIn(locations[0], result.results)
        self.assertIn(locations[1], result.results)
        self.assertIn(locations[2], result.results)
        self.assertIn(locations[3], result.results)
        data = result.results[locations[3]]
        self.assertEqual(len(data), 2)
