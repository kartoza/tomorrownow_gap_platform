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

from gap.utils.reader import (
    DatasetTimelineValue,
    DatasetReaderValue
)
from gap.utils.netcdf import (
    NetCDFProvider,
    daterange_inc,
    BaseNetCDFReader,
)
from gap.providers import (
    CBAMNetCDFReader,
    SalientNetCDFReader,
    get_reader_from_dataset
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

    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_client_kwargs')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_variables')
    @patch('fsspec.filesystem')
    def test_setupNetCDFReader(
        self, mock_filesystem, mock_get_s3_vars, mock_get_s3_kwargs):
        """Test for setup NetCDFReader class."""
        mock_get_s3_kwargs.return_value = {
            'endpoint_url': 'test_endpoint'
        }
        mock_get_s3_vars.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_key_id',
            'AWS_SECRET_ACCESS_KEY': 'test_key_secret',
        }
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.setup_netcdf_reader()
        mock_filesystem.assert_called_once_with(
            's3',
            key='test_key_id',
            secret='test_key_secret',
            client_kwargs=dict(endpoint_url='test_endpoint'))

    @patch('xarray.open_dataset')
    def test_open_dataset(self, mock_open_dataset):
        """Test for opening a dataset."""
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.fs = Mock()
        reader.s3 = {
            'AWS_DIR_PREFIX': '',
            'AWS_BUCKET_NAME': 'test_bucket'
        }
        netcdf_file = Mock()
        netcdf_file.name = 'test_file.nc'
        reader.open_dataset(netcdf_file)
        mock_open_dataset.assert_called_once()

    def test_from_dataset(self):
        """Test for creating NetCDFReader from dataset."""
        dataset1 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        reader = get_reader_from_dataset(dataset1)
        self.assertEqual(reader, CBAMNetCDFReader)
        dataset2 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        reader = get_reader_from_dataset(dataset2)
        self.assertEqual(reader, SalientNetCDFReader)
        # invalid type
        dataset3 = DatasetFactory.create()
        with self.assertRaises(TypeError):
            get_reader_from_dataset(dataset3)
