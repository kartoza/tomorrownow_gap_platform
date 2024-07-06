# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF sync task.
"""

import unittest
from unittest.mock import patch, MagicMock

from gap.models import (
    DatasetType,
    DatasetTimeStep,
    DatasetStore
)
from gap.utils.netcdf import NetCDFVariable, CBAM_VARIABLES, SALIENT_VARIABLES
from gap.tasks.netcdf_sync import (
    initialize_provider,
    initialize_provider_variables,
    sync_by_dataset,
    netcdf_s3_sync
)


class TestInitializeProvider(unittest.TestCase):
    """Unit test for initialize_provider functions."""

    @patch('gap.models.Provider.objects.get_or_create')
    @patch('gap.models.Dataset.objects.get_or_create')
    def test_initialize_provider_cbam(
        self, mock_get_or_create_dataset, mock_get_or_create_provider):
        """Test initialize CBAM provider."""
        provider_mock = MagicMock()
        provider_mock.name = 'CBAM'
        mock_get_or_create_provider.return_value = (provider_mock, False)
        dataset_mock = MagicMock()
        mock_get_or_create_dataset.return_value = (dataset_mock, False)

        provider, dataset = initialize_provider('CBAM')

        self.assertEqual(provider.name, 'CBAM')
        mock_get_or_create_provider.assert_called_with(name='CBAM')
        mock_get_or_create_dataset.assert_called_with(
            name='CBAM',
            provider=provider,
            defaults={
                'type': DatasetType.CLIMATE_REANALYSIS,
                'time_step': DatasetTimeStep.DAILY,
                'store_type': DatasetStore.NETCDF
            }
        )

    @patch('gap.models.Provider.objects.get_or_create')
    @patch('gap.models.Dataset.objects.get_or_create')
    def test_initialize_provider_salient(
        self, mock_get_or_create_dataset, mock_get_or_create_provider):
        """Test initialize salient provider."""
        provider_mock = MagicMock()
        provider_mock.name = 'Salient'
        mock_get_or_create_provider.return_value = (provider_mock, False)
        dataset_mock = MagicMock()
        mock_get_or_create_dataset.return_value = (dataset_mock, False)

        provider, dataset = initialize_provider('Salient')

        self.assertEqual(provider.name, 'Salient')
        mock_get_or_create_provider.assert_called_with(name='Salient')
        mock_get_or_create_dataset.assert_called_with(
            name='Salient',
            provider=provider,
            defaults={
                'type': DatasetType.SEASONAL_FORECAST,
                'time_step': DatasetTimeStep.DAILY,
                'store_type': DatasetStore.NETCDF
            }
        )


class TestInitializeProviderVariables(unittest.TestCase):
    """Unit test for initialize_provider_variables function."""

    @patch('gap.models.Unit.objects.get_or_create')
    @patch('gap.models.Attribute.objects.get_or_create')
    @patch('gap.models.DatasetAttribute.objects.get_or_create')
    def test_initialize_provider_variables(
        self, mock_get_or_create_dataset_attr, mock_get_or_create_attr,
        mock_get_or_create_unit):
        """Test initialize_provider_variables function."""
        dataset_mock = MagicMock()
        variables = {
            'temperature': NetCDFVariable(
                'Temperature', 'Temperature in Celsius', 'Celsius')
        }

        unit_mock = MagicMock()
        mock_get_or_create_unit.return_value = (unit_mock, False)
        attr_mock = MagicMock()
        mock_get_or_create_attr.return_value = (attr_mock, False)

        initialize_provider_variables(dataset_mock, variables)

        mock_get_or_create_unit.assert_called_with(name='Celsius')
        mock_get_or_create_attr.assert_called_with(
            name='Temperature',
            unit=unit_mock,
            variable_name='temperature',
            defaults={
                'description': 'Temperature in Celsius'
            }
        )
        mock_get_or_create_dataset_attr.assert_called_with(
            dataset=dataset_mock,
            attribute=attr_mock,
            source='temperature',
            source_unit=unit_mock
        )


class TestSyncByDataset(unittest.TestCase):
    """Unit test for sync_by_dataset function."""

    @patch('gap.tasks.netcdf_sync.s3fs.S3FileSystem')
    @patch('gap.models.NetCDFFile.objects.filter')
    @patch('gap.models.NetCDFFile.objects.create')
    @patch('gap.tasks.netcdf_sync.os.environ.get')
    def test_sync_by_dataset(
        self, mock_get_env, mock_create_netcdf_file, mock_filter_netcdf_file,
        mock_s3fs):
        """Test sync_by_dataset function."""
        mock_get_env.side_effect = (
            lambda key: 'test_bucket' if
            key == 'S3_AWS_BUCKET_NAME' else 'test_endpoint'
        )
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.walk.return_value = [
            ('s3://test_bucket/cbam', [], ['2023-01-01.nc'])
        ]
        mock_filter_netcdf_file.return_value.exists.return_value = False
        dataset_mock = MagicMock()
        dataset_mock.provider.name = 'cbam'

        sync_by_dataset(dataset_mock)

        mock_fs.walk.assert_called_with('s3://test_bucket/cbam')
        mock_filter_netcdf_file.assert_called()
        mock_create_netcdf_file.assert_called()


class TestNetCDFSyncTask(unittest.TestCase):
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
