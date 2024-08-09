# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading Zarr File
"""

import os
import logging
from typing import List
from datetime import datetime
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import fsspec

from gap.models import (
    Dataset,
    DatasetAttribute,
    DataSourceFile
)
from gap.utils.reader import (
    DatasetReaderInput
)
from gap.utils.netcdf import BaseNetCDFReader


logger = logging.getLogger(__name__)


class BaseZarrReader(BaseNetCDFReader):
    """Base class for Zarr Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime) -> None:
        """Initialize BaseZarrReader class.

        :param dataset: Dataset for reading
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)

    @classmethod
    def get_s3_variables(cls) -> dict:
        """Get s3 env variables for Zarr file.

        :return: Dictionary of S3 env vars
        :rtype: dict
        """
        prefix = 'MINIO'
        keys = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'AWS_ENDPOINT_URL', 'AWS_REGION_NAME'
        ]
        results = {}
        for key in keys:
            results[key] = os.environ.get(f'{prefix}_{key}', '')
        results['AWS_BUCKET_NAME'] = os.environ.get(
            'MINIO_GAP_AWS_BUCKET_NAME', '')
        results['AWS_DIR_PREFIX'] = os.environ.get(
            'MINIO_GAP_AWS_DIR_PREFIX', '')
        return results

    @classmethod
    def get_s3_client_kwargs(cls) -> dict:
        """Get s3 client kwargs for Zarr file.

        :return: dictionary with key endpoint_url or region_name
        :rtype: dict
        """
        prefix = 'MINIO'
        client_kwargs = {}
        if os.environ.get(f'{prefix}_AWS_ENDPOINT_URL', ''):
            client_kwargs['endpoint_url'] = os.environ.get(
                f'{prefix}_AWS_ENDPOINT_URL', '')
        if os.environ.get(f'{prefix}_AWS_REGION_NAME', ''):
            client_kwargs['region_name'] = os.environ.get(
                f'{prefix}_AWS_REGION_NAME', '')
        return client_kwargs

    @classmethod
    def get_zarr_base_url(cls, s3: dict) -> str:
        """Generate Zarr base URL.

        :param s3: Dictionary of S3 env vars
        :type s3: dict
        :return: Base URL with s3 and bucket name
        :rtype: str
        """
        prefix = s3['AWS_DIR_PREFIX']
        bucket_name = s3['AWS_BUCKET_NAME']
        zarr_url = f's3://{bucket_name}/{prefix}'
        if not zarr_url.endswith('/'):
            zarr_url += '/'
        return zarr_url

    def setup_reader(self):
        """Initialize s3fs."""
        self.s3 = self.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self.get_s3_client_kwargs()
        }

    def open_dataset(self, source_file: DataSourceFile) -> xrDataset:
        """Open a zarr file using xArray.

        :param source_file: zarr file from a dataset
        :type source_file: DataSourceFile
        :return: xArray Dataset object
        :rtype: xrDataset
        """
        zarr_url = self.get_zarr_base_url(self.s3)
        zarr_url += f'{source_file.name}'
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        return xr.open_zarr(s3_mapper)
