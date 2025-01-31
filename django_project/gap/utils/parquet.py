# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading and writing Parquet Files
"""


import os
import logging
import pandas as pd
import numpy as np
from django.db.models import F
from django.db.models.functions.datetime import (
    TruncDate,
    ExtractYear,
    TruncTime,
    ExtractMonth,
    ExtractDay
)
from django.contrib.gis.db.models import Union
from django.contrib.gis.db.models.functions import AsWKB
import duckdb
from django.conf import settings
from django.core.files.storage import storages

from gap.models import (
    Measurement, Dataset, DataSourceFile, Station,
    DatasetAttribute, StationHistory
)
from gap.providers.observation import ST_X, ST_Y
from gap.utils.ingestor_config import get_ingestor_config_from_preferences


logger = logging.getLogger(__name__)


class ParquetConverter:
    """Class to convert Measurement data to GeoParquet."""

    STATION_JOIN_KEY = 'st_id'
    WEATHER_FIELDS = {
        'date': (
            TruncDate('date_time')
        ),
        'time': (
            TruncTime('date_time')
        ),
        'attr': F('dataset_attribute__attribute__variable_name'),
        'st_id': F('station__id'),
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

    def __init__(
        self, dataset: Dataset, data_source: DataSourceFile, mode='w'
    ):
        """Initialize ParquetConverter class."""
        self.dataset = dataset
        self.mode = mode
        self.config = get_ingestor_config_from_preferences(dataset.provider)
        self.data_source = data_source
        self.attributes = [
            a.attribute.variable_name for a in
            DatasetAttribute.objects.select_related(
                'attribute'
            ).filter(
                dataset=dataset
            )
        ]

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

    def _get_connection(self, s3):
        endpoint = s3['AWS_ENDPOINT_URL']
        if settings.DEBUG:
            endpoint = endpoint.replace('http://', '')
        else:
            endpoint = endpoint.replace('https://', '')
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]

        config = {
            's3_access_key_id': s3['AWS_ACCESS_KEY_ID'],
            's3_secret_access_key': s3['AWS_SECRET_ACCESS_KEY'],
            's3_region': 'us-east-1',
            's3_url_style': 'path',
            's3_endpoint': endpoint,
            's3_use_ssl': not settings.DEBUG,
            'threads': 1
        }

        conn = duckdb.connect(config=config)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        return conn

    def _check_parquet_exists(self, s3_path: str, year: int):
        s3_storage = storages['gap_products']
        path = (
            f'{s3_path.replace(f's3://{self.s3['AWS_BUCKET_NAME']}/', '')}'
            f'year={year}'
        )
        _, files = s3_storage.listdir(path)
        return len(files) > 0

    def _store_dataframe_as_geoparquet(self, df: pd.DataFrame, s3_path, bbox):
        conn = self._get_connection(self.s3)
        # copy df to duckdb table
        columns = [c for c in list(df.columns) if c != 'geom']
        sql = (
            f"""
            CREATE TABLE weather AS
            SELECT {','.join(columns)}, ST_GeomFromWKB(geom) AS geometry
            FROM df
            ORDER BY
            ST_Hilbert(
                ST_GeomFromWKB(geom),
                ST_Extent(ST_MakeEnvelope(
                {bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}))
            );
            """
        )
        conn.sql(sql)
        # export to parquet file
        sql = (
            f"""
            COPY (SELECT * FROM weather)
            TO '{s3_path}'
            (FORMAT 'parquet', COMPRESSION 'zstd', PARTITION_BY (year),
            OVERWRITE_OR_IGNORE true);
            """
        )
        conn.sql(sql)
        conn.close()

    def _append_dataframe_to_geoparquet(
        self, df: pd.DataFrame, s3_path, bbox, year
    ):
        # TODO: check if parquet file for year exists
        conn = self._get_connection(self.s3)

        # copy original parquet to duckdb table
        sql = (
            f"""
            CREATE TABLE tmp_weather AS
            SELECT *
            FROM read_parquet('{s3_path}year=*/*.parquet',
            hive_partitioning=true)
            WHERE year={year}
            """
        )
        conn.sql(sql)

        # insert df to duckdb table
        columns = [c for c in list(df.columns) if c != 'geom']
        sql = (
            f"""
            INSERT INTO tmp_weather BY NAME
            SELECT {','.join(columns)}, ST_GeomFromWKB(geom) AS geometry
            FROM df
            """
        )
        conn.sql(sql)

        # order the table
        sql = (
            f"""
            CREATE TABLE weather AS
            SELECT * FROM tmp_weather
            ORDER BY
            ST_Hilbert(
                geometry,
                ST_Extent(ST_MakeEnvelope(
                {bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}))
            );
            """
        )
        conn.sql(sql)

        # export to parquet file
        sql = (
            f"""
            COPY (SELECT * FROM weather)
            TO '{s3_path}'
            (FORMAT 'parquet', COMPRESSION 'zstd', PARTITION_BY (year),
            OVERWRITE_OR_IGNORE true);
            """
        )
        conn.sql(sql)
        conn.close()

    def _get_station_bounds(self):
        combined_bbox = Station.objects.filter(
            provider=self.dataset.provider
        ).aggregate(
            combined_geometry=Union('geometry')
        )
        return combined_bbox['combined_geometry'].extent

    def _get_station_df(self, year):
        station_ids = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year
        ).distinct('station_id').values_list(
            'station_id',
            flat=True
        )

        stations = Station.objects.filter(
            id__in=station_ids
        ).order_by('id')
        fields = {
            'loc_x': ST_X('geometry'),
            'loc_y': ST_Y('geometry'),
            'st_id': F('id'),
            'st_code': F('code'),
            'iso_a3': F('country__iso_a3'),
            'geom': AsWKB('geometry'),
        }
        stations = stations.annotate(**fields).values(
            *(list(fields.keys()) + ['altitude', 'country_id'])
        )
        df = pd.DataFrame(list(stations))
        df['altitude'] = df['altitude'].astype('double')
        return df

    def _process_weather_df(self, year: int, measurements: list):
        # Convert to DataFrame
        df = pd.DataFrame(measurements)
        # Pivot the data to make attributes as columns
        df = df.pivot_table(
            index=[
                k for k in list(self.WEATHER_FIELDS.keys()) if k != 'attr'
            ],
            columns='attr',
            values='value'
        ).reset_index()
        print(f'Year {year} after pivot total_count: {df.shape[0]}')

        # add missing attributes
        missing_cols = []
        for attribute in self.attributes:
            if attribute in df.columns:
                continue
            missing_cols.append(
                pd.Series(
                    np.nan, dtype='double', index=df.index, name=attribute
                )
            )
        if missing_cols:
            print(f'Adding missing columns: {len(missing_cols)}')
            missing_cols.insert(0, df)
            df = pd.concat(missing_cols, axis=1)

        station_df = self._get_station_df(year)
        # merge df with station_df
        return df.merge(station_df, on=[self.STATION_JOIN_KEY], how='inner')

    def _process_year(self, year: int):
        measurements = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year
        ).order_by('date_time', 'station_id')
        print(f'Year {year} total_count: {measurements.count()}')

        measurements = measurements.annotate(**self.WEATHER_FIELDS).values(
            *(list(self.WEATHER_FIELDS.keys()) + ['value'])
        )

        return self._process_weather_df(year, list(measurements))

    def run(self):
        """Run the converter."""
        s3_path = self._get_directory_path(self.data_source)

        # get all distinct years
        years = list(Measurement.objects.annotate(
            year=ExtractYear('date_time')
        ).filter(
            dataset_attribute__dataset=self.dataset
        ).order_by('year').distinct('year').values_list(
            'year',
            flat=True
        ))

        station_bbox = self._get_station_bounds()
        for year in years:
            parquet_exists = self._check_parquet_exists(s3_path, year)
            print(f'data exists for year {year}: {parquet_exists}')
            df = self._process_year(year)

            if self.mode == 'a' and parquet_exists:
                self._append_dataframe_to_geoparquet(
                    df, s3_path, station_bbox, year
                )
            else:
                self._store_dataframe_as_geoparquet(df, s3_path, station_bbox)


class WindborneParquetConverter(ParquetConverter):
    """Class to convert WindborneMeasurement data to GeoParquet."""

    STATION_JOIN_KEY = 'st_hist_id'
    WEATHER_FIELDS = {
        'date': (
            TruncDate('date_time')
        ),
        'time': (
            TruncTime('date_time')
        ),
        'attr': F('dataset_attribute__attribute__variable_name'),
        'st_id': F('station__id'),
        'st_hist_id': F('station_history__id'),
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

    def __init__(self, dataset, data_source, mode='w'):
        """Initialize WindborneParquetConverter."""
        super().__init__(dataset, data_source, mode)

    def _get_station_bounds(self):
        combined_bbox = StationHistory.objects.filter(
            station__provider=self.dataset.provider
        ).aggregate(
            combined_geometry=Union('geometry')
        )
        return combined_bbox['combined_geometry'].extent

    def _get_station_df(self, year):
        station_hist_ids = Measurement.objects.filter(
            dataset_attribute__dataset=self.dataset,
            date_time__year=year
        ).distinct('station_history_id').values_list(
            'station_history_id',
            flat=True
        )

        stations = StationHistory.objects.filter(
            id__in=station_hist_ids
        ).order_by('id')
        fields = {
            'st_hist_id': F('id'),
            'loc_x': ST_X('geometry'),
            'loc_y': ST_Y('geometry'),
            'st_code': F('station__code'),
            'iso_a3': F('station__country__iso_a3'),
            'geom': AsWKB('geometry'),
            'country_id': F('station__country_id')
        }
        stations = stations.annotate(**fields).values(
            *(list(fields.keys()) + ['st_hist_id', 'altitude'])
        )
        df = pd.DataFrame(list(stations))
        df['altitude'] = df['altitude'].astype('double')
        return df
