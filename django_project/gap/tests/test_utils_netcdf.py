# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF Utilities.
"""

from django.test import TestCase
from datetime import datetime
import numpy as np
from django.contrib.gis.geos import Point
from unittest.mock import Mock, MagicMock, patch

from gap.utils.netcdf import (
    NetCDFProvider,
    daterange_inc,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseNetCDFReader,
    CBAMNetCDFReader,
    SalientNetCDFReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory
)


class TestNetCDFProvider(TestCase):
    """Unit test for NetCDFProvider class."""

    def test_constants(self):
        """Test for correct constants."""
        self.assertEqual(NetCDFProvider.CBAM, 'CBAM')
        self.assertEqual(NetCDFProvider.SALIENT, 'Salient')


class TestDaterangeInc(TestCase):
    """Unit test for daterange_inc function."""

    def test_daterange(self):
        """Test for daterance_inc function."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 3)
        expected_dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3)
        ]
        result_dates = list(daterange_inc(start_date, end_date))
        self.assertEqual(result_dates, expected_dates)


class TestDatasetTimelineValue(TestCase):
    """Unit test for class DatasetTimelineValue."""

    def test_to_dict(self):
        """Test convert to dict."""
        dt = np.datetime64('2023-01-01T00:00:00')
        values = {'temperature': 20.5}
        dtv = DatasetTimelineValue(dt, values)
        expected_dict = {
            'datetime': np.str_('2023-01-01T00:00:00Z'),
            'values': values
        }
        self.assertEqual(dtv.to_dict(), expected_dict)


class TestDatasetReaderValue(TestCase):
    """Unit test for class DatasetReaderValue."""

    def test_to_dict(self):
        """Test convert to dict."""
        metadata = {'source': 'test'}
        dtv1 = DatasetTimelineValue(
            np.datetime64('2023-01-01T00:00:00'), {'temp': 20.5})
        dtv2 = DatasetTimelineValue(
            np.datetime64('2023-01-02T00:00:00'), {'temp': 21.0})
        drv = DatasetReaderValue(metadata, [dtv1, dtv2])
        expected_dict = {
            'metadata': metadata,
            'data': [dtv1.to_dict(), dtv2.to_dict()]
        }
        self.assertEqual(drv.to_dict(), expected_dict)


class TestBaseNetCDFReader(TestCase):
    """Unit test for class BaseNetCDFReader."""

    def test_add_attribute(self):
        """Test adding a new attribute to Reader."""
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        self.assertEqual(len(reader.attributes), 0)
        reader.add_attribute(DatasetAttributeFactory.create())
        self.assertEqual(len(reader.attributes), 1)

    def test_read_variables(self):
        """Test reading variables."""
        dataset = DatasetFactory.create(name=NetCDFProvider.CBAM)
        attribute = AttributeFactory.create(
            name='Temperature', variable_name='temperature')
        dataset_attr = DatasetAttributeFactory(
            dataset=dataset, attribute=attribute)
        reader = BaseNetCDFReader(
            dataset, [dataset_attr], Point(x=29.125, y=-2.215),
            Mock(), Mock())
        xrArray = MagicMock()
        xrArray.sel.return_value = []
        xrDataset = MagicMock()
        xrDataset.__getitem__.return_value = xrArray
        result = reader.read_variables(xrDataset)
        self.assertEqual(result, [])

    @patch('os.environ.get')
    @patch('fsspec.filesystem')
    def test_setupNetCDFReader(self, mock_filesystem, mock_get):
        """Test for setup NetCDFReader class."""
        mock_get.side_effect = (
            lambda key: 'test_bucket' if
            key == 'S3_AWS_BUCKET_NAME' else 'test_endpoint'
        )
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.setupNetCDFReader()
        self.assertEqual(reader.bucket_name, 'test_bucket')
        mock_filesystem.assert_called_once_with(
            's3', client_kwargs=dict(endpoint_url='test_endpoint'))

    @patch('xarray.open_dataset')
    def test_open_dataset(self, mock_open_dataset):
        """Test for opening a dataset."""
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.bucket_name = 'test_bucket'
        reader.fs = Mock()
        netcdf_file = Mock()
        netcdf_file.name = 'test_file.nc'
        reader.open_dataset(netcdf_file)
        mock_open_dataset.assert_called_once()

    def test_from_dataset(self):
        """Test for creating NetCDFReader from dataset."""
        dataset1 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        reader = BaseNetCDFReader.from_dataset(dataset1)
        self.assertEqual(reader, CBAMNetCDFReader)
        dataset2 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        reader = BaseNetCDFReader.from_dataset(dataset2)
        self.assertEqual(reader, SalientNetCDFReader)
        # invalid type
        dataset3 = DatasetFactory.create()
        with self.assertRaises(TypeError):
            BaseNetCDFReader.from_dataset(dataset3)


class TestCBAMNetCDFReader(TestCase):
    """Unit test for CBAM NetCDFReader class."""

    @patch('gap.utils.netcdf.daterange_inc',
           return_value=[datetime(2023, 1, 1)])
    @patch('gap.models.NetCDFFile.objects.filter')
    def test_read_historical_data(self, mock_filter, mock_daterange_inc):
        """Test for reading historical data."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = CBAMNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        mock_filter.return_value.first.return_value = None
        reader.read_historical_data()
        self.assertEqual(reader.xrDatasets, [])

    def test_read_forecast_data_not_implemented(self):
        """Test for reading forecast data."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = CBAMNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        with self.assertRaises(NotImplementedError):
            reader.read_forecast_data()


class TestSalientNetCDFReader(TestCase):
    """Unit test for Salient NetCDFReader class."""

    def test_read_historical_data_not_implemented(self):
        """Test for reading historical data."""
        dataset = Mock()
        attributes = []
        point = Mock()
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        reader = SalientNetCDFReader(
            dataset, attributes, point, start_date, end_date)
        with self.assertRaises(NotImplementedError):
            reader.read_historical_data()

    @patch('gap.models.NetCDFFile.objects.filter')
    @patch('xarray.open_dataset')
    def test_read_forecast_data(self, mock_open_dataset, mock_filter):
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
        reader.read_forecast_data()
        self.assertEqual(reader.xrDatasets, [])
