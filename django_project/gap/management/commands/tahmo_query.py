# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run Tahmo query
"""

import logging
import os
import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from django.contrib.gis.geos import Point

import duckdb


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to run Tahmo query."""

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

    def setup_reader(self):
        """Initialize s3fs."""
        self.s3 = self._get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': self.get_s3_client_kwargs()
        }

    def _get_directory_path(self):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/tahmo_2/"
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
            'threads': 2
        }

        conn = duckdb.connect(config=config)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        return conn

    def handle(self, *args, **options):
        """Check Data Output."""
        self.setup_reader()
        parquet_path = self._get_directory_path()
        
        start_date = datetime.date(2019, 1, 1)
        end_date = datetime.date(2019, 1, 5)
        points = [
            Point(x=33.9,y=-4.67),
            Point(x=41.89,y=5.5),
        ]
        sql = (
            f"""
            SELECT date, loc_y as lat, loc_x as lon, st_id as station_id,
            min_air_temperature, max_air_temperature, precipitation, max_relative_humidity
            FROM read_parquet('{parquet_path}iso_a3=*/year=*/month=*/day=*/*.parquet')
            WHERE year>={start_date.year} AND month>={start_date.month} AND
            day>={start_date.day} AND
            year<={end_date.year} AND month<={end_date.month} AND
            day<={end_date.day} AND
            ST_Within(geometry, ST_MakeEnvelope(
            {points[0].x}, {points[0].y}, {points[1].x}, {points[1].y}))
            ORDER BY date
            """
        )

        conn = self._get_connection(self.s3)
        conn.sql(sql).show(max_rows=250)
        conn.close()
