# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF sync task.
"""

from django.test import TestCase
from unittest.mock import patch, MagicMock

from gap.models import (
    DatasetTimeStep,
    DatasetStore,
    Unit,
    Attribute,
    DatasetAttribute,
    DataSourceFile,
    CastType
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
    DataSourceFileFactory
)


class TestInitializeProvider(TestCase):
    """Unit test for initialize_provider functions."""

    def test_initialize_provider_cbam(self):
        """Test initialize CBAM provider."""
        provider, dataset = initialize_provider('CBAM')

        self.assertEqual(provider.name, 'CBAM')
        self.assertEqual(dataset.name, 'CBAM Climate Reanalysis')
        self.assertEqual(dataset.type.name, 'Climate Reanalysis')
        self.assertEqual(dataset.type.type, CastType.HISTORICAL)
        self.assertEqual(dataset.time_step, DatasetTimeStep.DAILY)
        self.assertEqual(dataset.store_type, DatasetStore.NETCDF)

    def test_initialize_provider_salient(self):
        """Test initialize salient provider."""
        provider, dataset = initialize_provider('Salient')

        self.assertEqual(provider.name, 'Salient')
        self.assertEqual(dataset.name, 'Salient Seasonal Forecast')
        self.assertEqual(dataset.type.name, 'Seasonal Forecast')
        self.assertEqual(dataset.type.type, CastType.FORECAST)
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
