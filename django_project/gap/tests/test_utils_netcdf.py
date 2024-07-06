# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF Utilities.
"""

import unittest
from datetime import datetime
import numpy as np
from unittest.mock import Mock, patch

from gap.utils.netcdf import (
    NetCDFProvider,
    daterange_inc,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseNetCDFReader,
    CBAMNetCDFReader,
    SalientNetCDFReader
)


class TestNetCDFProvider(unittest.TestCase):
    """Unit test for NetCDFProvider class."""

    def test_constants(self):
        """Test for correct constants."""
        self.assertEqual(NetCDFProvider.CBAM, 'CBAM')
        self.assertEqual(NetCDFProvider.SALIENT, 'Salient')


class TestDaterangeInc(unittest.TestCase):
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


class TestDatasetTimelineValue(unittest.TestCase):
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


class TestDatasetReaderValue(unittest.TestCase):
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


class TestBaseNetCDFReader(unittest.TestCase):
    """Unit test for class BaseNetCDFReader."""

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


class TestCBAMNetCDFReader(unittest.TestCase):
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


class TestSalientNetCDFReader(unittest.TestCase):
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
