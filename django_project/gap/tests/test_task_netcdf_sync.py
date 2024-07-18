# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF sync task.
"""

from django.test import TestCase
from unittest.mock import patch, MagicMock

from gap.models import (
    DataSourceFile
)
from gap.utils.netcdf import (
    NetCDFProvider
)
from gap.tasks.netcdf_sync import (
    sync_by_dataset,
    netcdf_s3_sync
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DataSourceFileFactory
)


class TestSyncByDataset(TestCase):
    """Unit test for sync_by_dataset function."""

    @patch('gap.tasks.netcdf_sync.s3fs.S3FileSystem')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_variables')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_client_kwargs')
    def test_sync_by_dataset(
        self, mock_get_s3_kwargs, mock_get_s3_env, mock_s3fs):
        """Test sync_by_dataset function."""
        mock_get_s3_env.return_value = {
            'AWS_DIR_PREFIX': 'cbam',
            'AWS_ENDPOINT_URL': 'test_endpoint',
            'AWS_BUCKET_NAME': 'test_bucket'
        }
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.walk.return_value = [
            ('test_bucket/cbam', [], ['2023-01-01.nc']),
            ('test_bucket/cbam', [], ['2023-01-02.nc']),
            ('test_bucket/cbam/dmrpp', [], ['2023-01-01.nc.dmrpp']),
            ('test_bucket/cbam/2023', [], ['2023-02-01.nc']),
        ]
        provider = ProviderFactory.create(name=NetCDFProvider.CBAM)
        dataset = DatasetFactory.create(provider=provider)
        # add existing NetCDF File
        DataSourceFileFactory.create(
            dataset=dataset,
            name='2023-01-02.nc'
        )
        sync_by_dataset(dataset)
        mock_fs.walk.assert_called_with('s3://test_bucket/cbam')
        self.assertEqual(
            DataSourceFile.objects.filter(
                dataset=dataset, name='2023-01-02.nc'
            ).count(),
            1
        )
        self.assertFalse(
            DataSourceFile.objects.filter(
                dataset=dataset, name='dmrpp/2023-01-01.nc.dmrpp'
            ).exists()
        )
        self.assertTrue(
            DataSourceFile.objects.filter(
                dataset=dataset, name='2023-01-01.nc'
            ).exists()
        )
        self.assertTrue(
            DataSourceFile.objects.filter(
                dataset=dataset, name='2023/2023-02-01.nc'
            ).exists()
        )


class TestNetCDFSyncTask(TestCase):
    """Unit test for netcdf_s3_sync function."""

    @patch('gap.tasks.netcdf_sync.sync_by_dataset')
    def test_netcdf_s3_sync(
        self, mock_sync_by_dataset):
        """Test for netcdf_s3_sync function."""
        cbam_dataset_mock = MagicMock()
        salient_dataset_mock = MagicMock()

        netcdf_s3_sync()
        mock_sync_by_dataset.assert_any_call(cbam_dataset_mock)
        mock_sync_by_dataset.assert_any_call(salient_dataset_mock)
