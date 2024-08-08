# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for CBAM Ingestor.
"""

from unittest.mock import patch, MagicMock
from datetime import datetime, date, time
import numpy as np
from xarray.core.dataset import Dataset as xrDataset
from django.test import TestCase

from gap.models import Dataset, DataSourceFile, DatasetStore
from gap.models.ingestor import IngestorSession
from gap.ingestor.cbam import CBAMIngestor


class CBAMIngestorTest(TestCase):
    """CBAM ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.observation_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Setup test."""
        self.dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')

    @patch('gap.utils.zarr.BaseZarrReader.get_s3_variables')
    @patch('gap.utils.zarr.BaseZarrReader.get_s3_client_kwargs')
    def test_init(self, mock_get_s3_client_kwargs, mock_get_s3_variables):
        """Test init method."""
        mock_get_s3_variables.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key'
        }
        mock_get_s3_client_kwargs.return_value = {
            'endpoint_url': 'https://test-endpoint.com'
        }
        session = IngestorSession.objects.create()
        ingestor = CBAMIngestor(session)
        self.assertEqual(ingestor.s3['AWS_ACCESS_KEY_ID'], 'test_access_key')
        self.assertEqual(ingestor.s3_options['key'], 'test_access_key')
        self.assertTrue(ingestor.datasource_file)

    def test_find_start_latlng(self):
        """Test find start latlng function."""
        metadata = {
            'original_min': -12.5969,
            'min': -27,
            'inc': 0.03574368
        }
        session = IngestorSession.objects.create()
        ingestor = CBAMIngestor(session)
        result = ingestor.find_start_latlng(metadata)
        expected_result = -27.00160304
        self.assertAlmostEqual(result, expected_result, places=6)

    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    def test_is_date_in_zarr(self, mock_open_dataset):
        """Test is_date_in_zarr function."""
        mock_ds = MagicMock()
        mock_ds.date.values = np.array([np.datetime64('2023-01-01')])
        mock_open_dataset.return_value = mock_ds

        session = IngestorSession.objects.create()
        ingestor = CBAMIngestor(session)
        ingestor.created = False
        ingestor.existing_dates = None
        self.assertTrue(ingestor.is_date_in_zarr(date(2023, 1, 1)))
        self.assertFalse(ingestor.is_date_in_zarr(date(2024, 1, 1)))

    @patch('gap.providers.CBAMNetCDFReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.open_dataset')
    @patch('gap.utils.zarr.BaseZarrReader.setup_reader')
    def test_run(
        self, mock_setup_reader, mock_open_dataset, mock_open_dataset_reader
    ):
        """Test run ingestor."""
        session = IngestorSession.objects.create()
        ingestor = CBAMIngestor(session)
        mock_ds = MagicMock(spec=xrDataset)
        mock_open_dataset.return_value = mock_ds
        mock_open_dataset_reader.return_value = mock_ds

        DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.NETCDF,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min)
        )

        ingestor.is_date_in_zarr = MagicMock(return_value=False)
        ingestor.store_as_zarr = MagicMock()

        ingestor.run()

        ingestor.store_as_zarr.assert_called_once_with(
            mock_ds, date(2023, 1, 1))

    @patch('json.dumps')
    def test_run_success(self, mock_json_dumps):
        """Test successful run."""
        session = IngestorSession.objects.create()
        ingestor = CBAMIngestor(session)
        ingestor._run = MagicMock()
        ingestor.metadata = {
            'start_date': date(2023, 1, 1),
            'end_date': date(2023, 12, 31),
            'total_processed': 10
        }
        datasourcefile = DataSourceFile.objects.create(
            name='test',
            dataset=self.dataset,
            start_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            end_date_time=datetime.combine(
                date=date(2023, 1, 1), time=time.min),
            format=DatasetStore.ZARR,
            created_on=datetime.combine(date=date(2023, 1, 1), time=time.min)
        )
        ingestor.datasource_file = datasourcefile
        ingestor.run()

        ingestor._run.assert_called_once()
        mock_json_dumps.assert_called_once_with(
            ingestor.metadata, default=str)
