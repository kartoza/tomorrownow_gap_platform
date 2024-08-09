# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Zarr Utilities.
"""

from django.test import TestCase
from datetime import datetime
from xarray.core.dataset import Dataset as xrDataset
from django.contrib.gis.geos import (
    Point
)
from unittest.mock import MagicMock, patch

from gap.models import Provider, Dataset, DatasetAttribute, DataSourceFile
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.providers import (
    CBAMZarrReader,
)


class TestCBAMZarrReader(TestCase):
    """Unit test for class TestCBAMZarrReader."""

    def setUp(self):
        """Set test for TestCBAMZarrReader."""
        self.provider = Provider(name='CBAM')
        self.dataset = Dataset(provider=self.provider)
        self.attributes = [DatasetAttribute(source='var1'),
                           DatasetAttribute(source='var2')]
        self.location_input = DatasetReaderInput.from_point(
            Point(1.0, 2.0)
        )
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = CBAMZarrReader(
            self.dataset, self.attributes, self.location_input,
            self.start_date, self.end_date
        )

    @patch.dict('os.environ', {
        'MINIO_AWS_ACCESS_KEY_ID': 'test_access_key',
        'MINIO_AWS_SECRET_ACCESS_KEY': 'test_secret_key',
        'MINIO_AWS_ENDPOINT_URL': 'https://test-endpoint.com',
        'MINIO_AWS_REGION_NAME': 'us-test-1',
        'MINIO_GAP_AWS_BUCKET_NAME': 'test-bucket',
        'MINIO_GAP_AWS_DIR_PREFIX': 'test-prefix/'
    })
    def test_get_s3_variables(self):
        """Test get_s3_variables function."""
        expected_result = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key',
            'AWS_ENDPOINT_URL': 'https://test-endpoint.com',
            'AWS_REGION_NAME': 'us-test-1',
            'AWS_BUCKET_NAME': 'test-bucket',
            'AWS_DIR_PREFIX': 'test-prefix/'
        }
        result = self.reader.get_s3_variables()
        self.assertEqual(result, expected_result)

    @patch.dict('os.environ', {
        'MINIO_AWS_ENDPOINT_URL': 'https://test-endpoint.com',
        'MINIO_AWS_REGION_NAME': 'us-test-1'
    })
    def test_get_s3_client_kwargs(self):
        """Test get_s3_client_kwargs function."""
        expected_result = {
            'endpoint_url': 'https://test-endpoint.com',
            'region_name': 'us-test-1'
        }
        result = self.reader.get_s3_client_kwargs()
        self.assertEqual(result, expected_result)

    def test_get_zarr_base_url(self):
        """Test get_zarr_base_url function."""
        s3 = {
            'AWS_DIR_PREFIX': 'test-prefix/',
            'AWS_BUCKET_NAME': 'test-bucket'
        }
        expected_result = 's3://test-bucket/test-prefix/'
        result = self.reader.get_zarr_base_url(s3)
        self.assertEqual(result, expected_result)

    @patch.dict('os.environ', {
        'MINIO_AWS_ACCESS_KEY_ID': 'test_access_key',
        'MINIO_AWS_SECRET_ACCESS_KEY': 'test_secret_key',
        'MINIO_AWS_ENDPOINT_URL': 'https://test-endpoint.com',
        'MINIO_AWS_REGION_NAME': 'us-test-1',
        'MINIO_GAP_AWS_BUCKET_NAME': 'test-bucket',
        'MINIO_GAP_AWS_DIR_PREFIX': 'test-prefix/'
    })
    @patch('fsspec.get_mapper')
    @patch('xarray.open_zarr')
    def test_open_dataset(self, mock_open_zarr, mock_get_mapper):
        """Test open zarr dataset function."""
        # Mocking the dependencies
        mock_mapper = MagicMock()
        mock_get_mapper.return_value = mock_mapper
        mock_dataset = MagicMock(spec=xrDataset)
        mock_open_zarr.return_value = mock_dataset

        source_file = DataSourceFile(name='test.zarr')
        self.reader.setup_reader()
        result = self.reader.open_dataset(source_file)

        # Assertions to ensure the method is called correctly
        mock_get_mapper.assert_called_once_with(
            's3://test-bucket/test-prefix/test.zarr',
            key='test_access_key',
            secret='test_secret_key',
            client_kwargs={
                'endpoint_url': 'https://test-endpoint.com',
                'region_name': 'us-test-1'
            }
        )
        mock_open_zarr.assert_called_once_with(mock_mapper)
        self.assertEqual(result, mock_dataset)

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_setup_reader(
        self, mock_get_s3_client_kwargs, mock_get_s3_variables
    ):
        """Test setup_reader function."""
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key',
            'AWS_ENDPOINT_URL': 'https://test-endpoint.com',
            'AWS_REGION_NAME': 'us-test-1'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
        }
        self.reader.setup_reader()

        self.assertEqual(
            self.reader.s3['AWS_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(self.reader.s3_options['key'], 'test_access_key')
        self.assertEqual(self.reader.s3_options['secret'], 'test_secret_key')
        self.assertEqual(
            self.reader.s3_options['client_kwargs']['endpoint_url'],
            'https://test-endpoint.com'
        )
