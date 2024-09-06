# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Zarr Utilities.
"""

from django.test import TestCase
from datetime import datetime
from xarray.core.dataset import Dataset as xrDataset
from django.contrib import messages
from django.contrib.gis.geos import (
    Point
)
from unittest.mock import MagicMock, patch

from gap.models import (
    Provider, Dataset, DatasetAttribute, DataSourceFile,
    DatasetStore
)
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.providers import (
    CBAMZarrReader,
)
from gap.admin.main import (
    load_source_zarr_cache,
    clear_source_zarr_cache
)
from gap.factories import DataSourceFileFactory


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
    @patch('xarray.open_zarr')
    @patch('fsspec.filesystem')
    @patch('s3fs.S3FileSystem')
    def test_open_dataset(
        self, mock_s3fs, mock_fsspec_filesystem, mock_open_zarr
    ):
        """Test open zarr dataset function."""
        # Mock the s3fs.S3FileSystem constructor
        mock_s3fs_instance = MagicMock()
        mock_s3fs.return_value = mock_s3fs_instance

        # Mock the fsspec.filesystem constructor
        mock_fs_instance = MagicMock()
        mock_fsspec_filesystem.return_value = mock_fs_instance

        # Mock the xr.open_zarr function
        mock_dataset = MagicMock(spec=xrDataset)
        mock_open_zarr.return_value = mock_dataset

        source_file = DataSourceFile(name='test_dataset.zarr')
        self.reader.setup_reader()
        result = self.reader.open_dataset(source_file)

        # Assertions to ensure the method is called correctly
        assert result == mock_dataset
        mock_s3fs.assert_called_once_with(
            key='test_access_key',
            secret='test_secret_key',
            endpoint_url='https://test-endpoint.com'
        )
        cache_filename = 'test_dataset_zarr'
        mock_fsspec_filesystem.assert_called_once_with(
            'filecache',
            target_protocol='s3',
            target_options=self.reader.s3_options,
            cache_storage=f'/tmp/{cache_filename}',
            cache_check=10,
            expiry_time=3600,
            target_kwargs={'s3': mock_s3fs_instance}
        )
        mock_fs_instance.get_mapper.assert_called_once_with(
            's3://test-bucket/test-prefix/test_dataset.zarr')
        mock_open_zarr.assert_called_once_with(
            mock_fs_instance.get_mapper.return_value,
            consolidated=True)

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


class TestAdminZarrFileActions(TestCase):
    """Test for actions for Zarr DataSourceFile."""

    fixtures = [
        '2.provider.json',
        '3.observation_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.setup_reader')
    def test_load_source_zarr_cache(
        self, mock_setup_reader, mock_open_dataset
    ):
        """Test load cache for DataSourceFile zarr."""
        # Mock the queryset with a Zarr file
        data_source = DataSourceFileFactory.create(
            format=DatasetStore.ZARR,
            name='test.zarr'
        )
        mock_queryset = [data_source]

        # Mock the modeladmin and request objects
        mock_modeladmin = MagicMock()
        mock_request = MagicMock()

        # Call the load_source_zarr_cache function
        load_source_zarr_cache(mock_modeladmin, mock_request, mock_queryset)

        # Assertions
        mock_setup_reader.assert_called_once()
        mock_open_dataset.assert_called_once_with(data_source)
        mock_modeladmin.message_user.assert_called_once_with(
            mock_request, 'test.zarr zarr cache has been loaded!',
            messages.SUCCESS
        )

    @patch('shutil.rmtree')
    @patch('os.path.exists')
    def test_clear_source_zarr_cache(self, mock_os_path_exists, mock_rmtree):
        """Test clear cache DataSourceFile zarr."""
        # Mock the queryset with a Zarr file
        data_source = DataSourceFileFactory.create(
            format=DatasetStore.ZARR,
            name='test.zarr'
        )
        mock_queryset = [data_source]

        # Mock os.path.exists to return True
        mock_os_path_exists.return_value = True

        # Mock the modeladmin and request objects
        mock_modeladmin = MagicMock()
        mock_request = MagicMock()

        # Call the clear_source_zarr_cache function
        clear_source_zarr_cache(mock_modeladmin, mock_request, mock_queryset)

        # Assertions
        mock_os_path_exists.assert_called_once_with('/tmp/test_zarr')
        mock_rmtree.assert_called_once_with('/tmp/test_zarr')
        mock_modeladmin.message_user.assert_called_once_with(
            mock_request,
            '/tmp/test_zarr has been cleared!',
            messages.SUCCESS
        )
