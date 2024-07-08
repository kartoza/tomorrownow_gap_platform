# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF sync task.
"""

from django.test import TestCase
from unittest.mock import patch, MagicMock

from gap.models import (
    DatasetType,
    DatasetTimeStep,
    DatasetStore,
    Unit,
    Attribute,
    DatasetAttribute,
    NetCDFFile
)
from gap.utils.netcdf import (
    NetCDFProvider, NetCDFVariable, CBAM_VARIABLES, SALIENT_VARIABLES
)
from gap.tasks.netcdf_sync import (
    initialize_provider,
    initialize_provider_variables,
    sync_by_dataset,
    netcdf_s3_sync
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    NetCDFFileFactory
)


class TestInitializeProvider(TestCase):
    """Unit test for initialize_provider functions."""

    def test_initialize_provider_cbam(self):
        """Test initialize CBAM provider."""
        provider, dataset = initialize_provider('CBAM')

        self.assertEqual(provider.name, 'CBAM')
        self.assertEqual(dataset.name, 'CBAM')
        self.assertEqual(dataset.type, DatasetType.CLIMATE_REANALYSIS)
        self.assertEqual(dataset.time_step, DatasetTimeStep.DAILY)
        self.assertEqual(dataset.store_type, DatasetStore.NETCDF)

    def test_initialize_provider_salient(self):
        """Test initialize salient provider."""
        provider, dataset = initialize_provider('Salient')

        self.assertEqual(provider.name, 'Salient')
        self.assertEqual(dataset.name, 'Salient')
        self.assertEqual(dataset.type, DatasetType.SEASONAL_FORECAST)
        self.assertEqual(dataset.time_step, DatasetTimeStep.DAILY)
        self.assertEqual(dataset.store_type, DatasetStore.NETCDF)


class TestInitializeProviderVariables(TestCase):
    """Unit test for initialize_provider_variables function."""

    def test_initialize_provider_variables(self):
        """Test initialize_provider_variables function."""
        dataset = DatasetFactory(name=NetCDFProvider.CBAM)
        variables = {
            'temperature': NetCDFVariable(
                'Temperature', 'Temperature in Celsius', 'Celsius')
        }
        initialize_provider_variables(dataset, variables)
        self.assertTrue(Unit.objects.filter(name='Celsius').exists())
        self.assertTrue(Attribute.objects.filter(
            name='Temperature',
            unit__name='Celsius'
        ).exists())
        self.assertTrue(DatasetAttribute.objects.filter(
            dataset=dataset,
            attribute__name='Temperature',
            source='temperature',
            source_unit__name='Celsius'
        ).exists())


class TestSyncByDataset(TestCase):
    """Unit test for sync_by_dataset function."""

    @patch('gap.tasks.netcdf_sync.s3fs.S3FileSystem')
    @patch('gap.tasks.netcdf_sync.os.environ.get')
    def test_sync_by_dataset(self, mock_get_env, mock_s3fs):
        """Test sync_by_dataset function."""
        mock_get_env.side_effect = (
            lambda key: 'test_bucket' if
            key == 'S3_AWS_BUCKET_NAME' else 'test_endpoint'
        )
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.walk.return_value = [
            ('test_bucket/cbam', [], ['2023-01-01.nc']),
            ('test_bucket/cbam', [], ['2023-01-02.nc']),
            ('test_bucket/cbam/dmrpp', [], ['2023-01-01.nc.dmrpp'])
        ]
        provider = ProviderFactory.create(name=NetCDFProvider.CBAM)
        dataset = DatasetFactory.create(provider=provider)
        # add existing NetCDF File
        NetCDFFileFactory.create(
            dataset=dataset,
            name='cbam/2023-01-02.nc'
        )
        sync_by_dataset(dataset)
        mock_fs.walk.assert_called_with('s3://test_bucket/cbam')
        self.assertEqual(
            NetCDFFile.objects.filter(
                dataset=dataset,
                name='cbam/2023-01-02.nc'
            ).count(),
            1
        )
        self.assertFalse(
            NetCDFFile.objects.filter(
                dataset=dataset,
                name='cbam/dmrpp/2023-01-01.nc.dmrpp'
            ).exists()
        )
        self.assertTrue(
            NetCDFFile.objects.filter(
                dataset=dataset,
                name='cbam/2023-01-01.nc'
            ).exists()
        )


class TestNetCDFSyncTask(TestCase):
    """Unit test for netcdf_s3_sync function."""

    @patch('gap.tasks.netcdf_sync.initialize_provider')
    @patch('gap.tasks.netcdf_sync.initialize_provider_variables')
    @patch('gap.tasks.netcdf_sync.sync_by_dataset')
    def test_netcdf_s3_sync(
        self, mock_sync_by_dataset, mock_initialize_provider_variables,
        mock_initialize_provider):
        """Test for netcdf_s3_sync function."""
        cbam_provider_mock = MagicMock()
        salient_provider_mock = MagicMock()
        cbam_dataset_mock = MagicMock()
        salient_dataset_mock = MagicMock()
        mock_initialize_provider.side_effect = [
            (cbam_provider_mock, cbam_dataset_mock),
            (salient_provider_mock, salient_dataset_mock)
        ]

        netcdf_s3_sync()

        mock_initialize_provider.assert_any_call('CBAM')
        mock_initialize_provider.assert_any_call('Salient')
        mock_initialize_provider_variables.assert_any_call(
            cbam_dataset_mock, CBAM_VARIABLES)
        mock_initialize_provider_variables.assert_any_call(
            salient_dataset_mock, SALIENT_VARIABLES)
        mock_sync_by_dataset.assert_any_call(cbam_dataset_mock)
        mock_sync_by_dataset.assert_any_call(salient_dataset_mock)
