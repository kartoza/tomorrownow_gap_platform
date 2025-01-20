# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Outputs
"""

import os
import shutil
import fsspec
import pandas as pd
from dask.dataframe.core import DataFrame as dask_df
import dask_geopandas as dg
from dask_geopandas.io.parquet import to_parquet
from typing import Union

from gap.utils.dask import execute_dask_compute


class OutputType:
    """Enum class for output type."""

    GRID_DATA = 1
    GRID_CROP_DATA = 2
    FARM_CROP_DATA = 3


class DCASPipelineOutput:
    """Class to manage pipeline output."""

    TMP_BASE_DIR = '/tmp/dcas'

    def __init__(self, request_date):
        """Initialize DCASPipelineOutput."""
        self.fs = None
        self.request_date = request_date

    def setup(self):
        """Set DCASPipelineOutput."""
        self._setup_s3fs()

        # clear temp resource
        if os.path.exists(self.TMP_BASE_DIR):
            shutil.rmtree(self.TMP_BASE_DIR)
        os.makedirs(self.TMP_BASE_DIR, exist_ok=True)

    def cleanup(self):
        """Remove temporary directory."""
        if os.path.exists(self.TMP_BASE_DIR):
            shutil.rmtree(self.TMP_BASE_DIR)

    @property
    def grid_data_file_path(self):
        """Return full path to grid data output parquet file."""
        return os.path.join(
            self.TMP_BASE_DIR,
            'grid_data.parquet'
        )

    @property
    def grid_crop_data_dir_path(self):
        """Return full path to directory grid with crop data."""
        return os.path.join(
            self.TMP_BASE_DIR,
            'grid_crop'
        )

    @property
    def grid_crop_data_path(self):
        """Return full path to grid with crop data."""
        return self.grid_crop_data_dir_path + '/*.parquet'

    def _setup_s3fs(self):
        """Initialize s3fs."""
        self.s3 = self._get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self._get_s3_client_kwargs()
        }
        self.fs = fsspec.filesystem(
            's3',
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=self._get_s3_client_kwargs()
        )

    def _get_s3_variables(self) -> dict:
        """Get s3 env variables for product bucket.

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

    def _get_s3_client_kwargs(self) -> dict:
        """Get s3 client kwargs for parquet file.

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

    def _get_directory_path(self, directory_name):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/{directory_name}"
        )

    def save(self, type: int, df: Union[pd.DataFrame, dask_df]):
        """Save output to parquet files.

        :param type: Type of the dataframe output
        :type type: int
        :param df: DataFrame output
        :type df: Union[pd.DataFrame, dask_df]
        :raises ValueError: Raise when there is invalid type
        """
        if type == OutputType.GRID_DATA:
            self._save_grid_data(df)
        elif type == OutputType.GRID_CROP_DATA:
            self._save_grid_crop_data(df)
        elif type == OutputType.FARM_CROP_DATA:
            self._save_farm_crop_data(df)
        else:
            raise ValueError(f'Invalid output type {type} to be saved!')

    def _save_farm_crop_data(self, df: dask_df, directory_name='dcas_output'):
        df_geo = dg.from_dask_dataframe(
            df,
            geometry=dg.from_wkb(df['geometry'])
        )

        print('Saving to parquet')

        x = to_parquet(
            df_geo,
            self._get_directory_path(directory_name),
            partition_on=['iso_a3', 'year', 'month', 'day'],
            filesystem=self.fs,
            compute=False
        )
        print(f'writing to {self._get_directory_path(directory_name)}')
        execute_dask_compute(x)

    def _save_grid_crop_data(self, df: dask_df):
        dir_path = self.grid_crop_data_dir_path
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)

        print('Saving to parquet')

        df = df.reset_index(drop=True)
        x = df.to_parquet(
            dir_path,
            compute=False
        )
        print(f'writing to {dir_path}')
        execute_dask_compute(x)

    def _save_grid_data(self, df: pd.DataFrame):
        file_path = self.grid_data_file_path
        print(f'writing dataframe to {file_path}')
        df.to_parquet(file_path)
