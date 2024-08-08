# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading Zarr File
"""

import os
import json
import logging
from typing import List
from datetime import datetime, timedelta
from django.contrib.gis.geos import Point
import numpy as np
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import fsspec
from shapely.geometry import shape

from gap.models import (
    Provider,
    Dataset,
    DatasetAttribute,
    DataSourceFile
)
from gap.utils.reader import (
    LocationInputType,
    BaseDatasetReader,
    DatasetReaderInput
)
from gap.utils.netcdf import BaseNetCDFReader


logger = logging.getLogger(__name__)


class BaseZarrReader(BaseNetCDFReader):

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
    def get_s3_variables(cls):
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
    def get_s3_client_kwargs(cls):
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
    def get_zarr_base_url(cls, s3: dict):
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
