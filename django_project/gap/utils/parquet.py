# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading and writing Parquet Files
"""


import os
import logging
import pandas as pd
import fsspec
from django.db.models import F
from django.db.models.functions.datetime import (
    TruncDate,
    ExtractYear,
    TruncTime,
    ExtractMonth,
    ExtractDay
)
from django.contrib.gis.db.models.functions import AsWKB
import dask_geopandas as dg
from dask_geopandas.io.parquet import to_parquet
import dask.dataframe as dd

from gap.models import (
    Measurement, Dataset, DataSourceFile
)
from gap.providers.observation import ST_X, ST_Y
from gap.utils.dask import execute_dask_compute
from gap.utils.ingestor_config import get_ingestor_config_from_preferences


logger = logging.getLogger(__name__)


class ParquetConverter:
    """Class to convert Measurement data to GeoParquet."""

    DEFAULT_NUM_PARTITIONS = 1

    def __init__(self, dataset: Dataset):
        """Initialize ParquetConverter class."""
        self.dataset = dataset
        self.config = get_ingestor_config_from_preferences(dataset.provider)

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

    def _get_s3_client_kwargs(cls) -> dict:
        """Get s3 client kwargs for savign parquet file.

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

    def setup(self):
        """Initialize s3fs."""
        self.s3 = self._get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self._get_s3_client_kwargs()
        }

    def _get_directory_path(self, data_source: DataSourceFile):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/{data_source.name}/"
        )

    def _get_data_source_file(self) -> DataSourceFile:
        return None

    def _store_dataframe_as_geoparquet(self, df: pd.DataFrame, fs, s3_path):
        # create dask dataframe
        ddf = dd.from_pandas(df, npartitions=self.DEFAULT_NUM_PARTITIONS)

        # Create a GeoDataFrame
        df_geo = dg.from_dask_dataframe(
            ddf,
            geometry=dg.from_wkb(ddf['geometry'])
        )

        # Save to GeoParquet files
        print('Saving to parquet')
        x = to_parquet(
            df_geo,
            s3_path,
            partition_on=['iso_a3', 'year', 'month', 'day'],
            filesystem=fs,
            compute=False
        )
        print(f'writing to {s3_path}')
        execute_dask_compute(x)

    def _process_year(self, year: int, fs, s3_path):
        fields = {
            'date': (
                TruncDate('date_time')
            ),
            'time': (
                TruncTime('date_time')
            ),
            'loc_x': ST_X('geom'),
            'loc_y': ST_Y('geom'),
            'altitude': F('station__altitude'),
            'attr': F('dataset_attribute__attribute__variable_name'),
            'st_code': F('station__code'),
            'st_id': F('station__id'),
            'country': F('station__country'),
            'iso_a3': F('station__country__iso_a3'),
            'geometry': AsWKB('geom'),
            'last_loc_x': ST_X('geom'),
            'last_loc_y': ST_Y('geom'),
            'year': (
                ExtractYear('date_time')
            ),
            'month': (
                ExtractMonth('date_time')
            ),
            'day': (
                ExtractDay('date_time')
            ),
        }
        # TODO: for windborne, the station should be using station_history
        # and last_loc_x, last_loc_y would be the latest station location
        measurements = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year
        ).order_by('date_time', 'station_id')
        print(f'Year {year} total_count: {measurements.count()}')

        measurements = measurements.annotate(
            geom=F('station__geometry'),
        ).annotate(**fields).values(
            *(list(fields.keys()) + ['value'])
        )

        # Convert to DataFrame
        df = pd.DataFrame(list(measurements))

        # Pivot the data to make attributes columns
        df = df.pivot_table(
            index=list(fields.keys()) - ['attr'],
            columns='attr',
            values='value'
        ).reset_index()

        # # Extract year, month, and day from the date column
        # df['year'] = df['date'].apply(lambda x: x.year)
        # df['month'] = df['date'].apply(lambda x: x.month)
        # df['day'] = df['date'].apply(lambda x: x.day)

        self._store_dataframe_as_geoparquet(df, fs, s3_path)

    def run(self):
        """Run the converter."""
        fs = fsspec.filesystem(
            's3',
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=self._get_s3_client_kwargs()
        )
        data_source_file = self._get_data_source_file()
        s3_path = self._get_directory_path(data_source_file)

        # get all distinct years
        years = list(Measurement.objects.annotate(
            year=ExtractYear('date_time')
        ).filter(
            dataset_attribute__dataset=self.dataset
        ).order_by('year').distinct('year').values_list(
            'year',
            flat=True
        ))

        for year in years:
            self._process_year(year, fs, s3_path)
