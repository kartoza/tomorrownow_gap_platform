# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Data Pipeline
"""


import logging
import os
import random
import uuid
import datetime
import fsspec
import numpy as np
import pandas as pd
from pprint import pprint
from django.db import connection
from django.db.models import Min
import dask.dataframe as dd
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, select, distinct, column, cast
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2.functions import ST_X, ST_Y, ST_Centroid
from geoalchemy2.types import Geometry
import dask_geopandas as dg
import geopandas as gpd
from shapely import from_wkb
from dask_geopandas.io.parquet import to_parquet

from gap.utils.dask import execute_dask_compute
from gap.models import FarmRegistryGroup, FarmRegistry
from dcas.models import DCASConfig
from dcas.rules.rule_engine import DCASRuleEngine


logger = logging.getLogger(__name__)


class DCASDataPipeline:
    """Class for DCAS data pipeline."""

    NUM_PARTITIONS = 50
    LIMIT = 100000

    def __init__(
        self, farm_registry_group: FarmRegistryGroup,
        config: DCASConfig,
        request_date: datetime.date
    ):
        """Initialize DCAS Data Pipeline.

        :param farm_registry_group: _description_
        :type farm_registry_group: FarmRegistryGroup
        :param config: _description_
        :type config: DCASConfig
        """
        self.farm_registry_group = farm_registry_group
        self.config = config
        self.rule_engine = DCASRuleEngine(self.config)
        self.base_schema = None
        self.fs = None
        self.minimum_plant_date = None
        self.crops = []
        self.request_date = request_date
        self.max_temperature_epoch = []

    def setup(self):
        """Set the data pipeline."""
        self.rule_engine.initialize()
        self._init_schema()

        self._setup_s3fs()

        # fetch minimum plant date
        self.minimum_plant_date: datetime.date = FarmRegistry.objects.filter(
            group=self.farm_registry_group
        ).aggregate(Min('planting_date'))['planting_date__min']
        # fetch crop id list
        farm_qs = FarmRegistry.objects.filter(
            group=self.farm_registry_group
        ).order_by('crop_id').values_list('crop_id', flat=True).distinct('crop_id')
        self.crops = list(farm_qs)

        print(self.minimum_plant_date)
        print(self.crops)
        

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

    def _get_directory_path(self):
        return (
            f"s3://{self.s3['AWS_BUCKET_NAME']}/"
            f"{self.s3['AWS_DIR_PREFIX']}/test_dcas"
        )

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

    def _conn_str(self):
        return 'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}'.format(
            **connection.settings_dict
        )

    def _init_schema(self):
        # Create an SQLAlchemy engine
        engine = create_engine(self._conn_str())

        # Use automap base
        self.base_schema = automap_base()

        # Reflect the tables
        self.base_schema.prepare(engine, reflect=True)

        # Access reflected tables as classes
        # for table_name, mapped_class in self.base_schema.classes.items():
        #     if table_name != 'gap_farmregistry':
        #         continue
        #     print(f"Table: {table_name}, Class: {mapped_class}")
        #     pprint(vars(mapped_class.__table__))
        #     break
    
        # all accessed tables here
        self.farmregistry = self.base_schema.classes['gap_farmregistry'].__table__
        self.farm = self.base_schema.classes['gap_farm'].__table__
        self.cropstagetype = self.base_schema.classes['gap_cropstagetype'].__table__
        self.cropgrowthstage = self.base_schema.classes['gap_cropgrowthstage'].__table__
        self.crop = self.base_schema.classes['gap_crop'].__table__
        self.grid = self.base_schema.classes['gap_grid'].__table__
        self.country = self.base_schema.classes['gap_country'].__table__

    def load_grid_data(self) -> pd.DataFrame:
        subquery = select(
            self.grid.c.id.label('grid_id'),
            ST_Centroid(self.grid.c.geometry).label('centroid')
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).where(
            self.farmregistry.c.group_id == self.farm_registry_group.id
        ).order_by(
            self.grid.c.id
        )
        
        if self.LIMIT:
            # for testing purpose
            subquery = subquery.limit(self.LIMIT)

        subquery = subquery.subquery('grid_data')
        sql_query = select(
            distinct(column('grid_id')),
            ST_Y(column('centroid')).label('lat'),
            ST_X(column('centroid')).label('lon'),
        ).select_from(subquery)
        print(sql_query)

        return pd.read_sql_query(
            sql_query,
            con=self._conn_str(),
            index_col='grid_id',
        )

    def load_grid_weather_data(self, df) -> pd.DataFrame:
        self.max_temperature_epoch = []
        columns = {}
        dates = pd.date_range(
            self.minimum_plant_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )

        for date in dates:
            epoch = int(date.timestamp())
            columns[f'max_temperature_{epoch}'] = lambda df: np.random.uniform(low=25, high=40, size=df.shape[0])
            columns[f'min_temperature_{epoch}'] = lambda df: np.random.uniform(low=5, high=15, size=df.shape[0])
            columns[f'total_rainfall_{epoch}'] = lambda df: np.random.random(df.shape[0])
            self.max_temperature_epoch.append(epoch)

        dates_humidity = pd.date_range(
            self.request_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates_humidity:
            epoch = int(date.timestamp())
            columns[f'max_humidity_{epoch}'] = lambda df: np.random.random(df.shape[0])
            columns[f'min_humidity_{epoch}'] = lambda df: np.random.random(df.shape[0])

        dates_precip = pd.date_range(
            self.request_date - datetime.timedelta(days=6),
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates_precip:
            epoch = int(date.timestamp())
            columns[f'precipitation_{epoch}'] = lambda df: np.random.random(df.shape[0])
            columns[f'evapotranspiration_{epoch}'] = lambda df: np.random.random(df.shape[0])

        return df.assign(**columns)

    def merge_grid_data_with_crop(self, df: pd.DataFrame) -> pd.DataFrame:
        df_crop = pd.DataFrame({
            'crop_id': self.crops,
            'gdd_base': [random.uniform(5, 15) for crop in self.crops],
            'gdd_cap': [random.uniform(25, 40) for crop in self.crops]
        })
        
        # Create a Cartesian product using merge with a key
        df['key'] = 1
        df_crop['key'] = 1

        return pd.merge(df, df_crop, on='key').drop('key', axis=1)

    def calculate_gdd(self, df: pd.DataFrame) -> pd.DataFrame:
        intermediate_columns = []
        # add new column to normalize max and min temperature
        for epoch in self.max_temperature_epoch:
            df[f'norm_max_temp_{epoch}'] = df[f'max_temperature_{epoch}'].clip(upper=df['gdd_cap'])
            df[f'norm_min_temp_{epoch}'] = df[f'min_temperature_{epoch}'].clip(lower=df['gdd_base'])
            intermediate_columns.append(f'norm_max_temp_{epoch}')
            intermediate_columns.append(f'norm_min_temp_{epoch}')

        for epoch in self.max_temperature_epoch:
            df[f'gdd_{epoch}'] = ((df[f'norm_max_temp_{epoch}'] + df[f'norm_min_temp_{epoch}']) / 2) - df['gdd_base']

        df = df.drop(columns=intermediate_columns)
        return df

    def load_farm_registry_data(self) -> dd:
        subquery = select(
            self.farmregistry.c.id.label('farmregistry_id'),
            self.farmregistry.c.planting_date.label('planting_date'),
            self.farmregistry.c.crop_id.label('crop_id'),
            self.farmregistry.c.crop_growth_stage_id.label('crop_growth_stage_id'),
            self.farmregistry.c.crop_stage_type_id.label('crop_stage_type_id'),
            self.farmregistry.c.growth_stage_start_date.label('growth_stage_start_date'),
            self.farmregistry.c.group_id,
            self.farm.c.id.label('farm_id'),
            self.farm.c.unique_id.label('farm_unique_id'),
            self.farm.c.geometry.label('geometry'),
            self.grid.c.id.label('grid_id'),
            self.grid.c.unique_id.label('grid_unique_id'),
            self.country.c.name.label('country'),
            self.country.c.iso_a3.label('iso_a3'),
            self.farmregistry.c.id.label('registry_id'),
            self.cropgrowthstage.c.name.label('growth_stage'),
            (self.crop.c.name + '_' + self.cropstagetype.c.name).label('crop')
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).join(
            self.country, self.grid.c.country_id == self.country.c.id
        ).join(
            self.crop, self.farmregistry.c.crop_id == self.crop.c.id
        ).join(
            self.cropstagetype,
            self.farmregistry.c.crop_stage_type_id == self.cropstagetype.c.id
        ).join(
            self.cropgrowthstage,
            self.farmregistry.c.crop_growth_stage_id == self.cropgrowthstage.c.id, isouter=True
        ).where(
            self.farmregistry.c.group_id == self.farm_registry_group.id
        ).order_by(
            self.grid.c.id, self.farmregistry.c.id
        )
        
        if self.LIMIT:
            # for testing purpose
            subquery = subquery.limit(self.LIMIT)

        subquery = subquery.subquery('farm_data')

        sql_query = select(subquery)

        return dd.read_sql_query(
            sql=sql_query,
            con=self._conn_str(),
            index_col='farmregistry_id',
            npartitions=self.NUM_PARTITIONS,
        )

    def merge_farm_registry_grid_data(self, farm_df: dd, grid_df: pd.DataFrame) -> dd:
        return farm_df.map_partitions(lambda part: part.merge(grid_df, on=['grid_id', 'crop_id']))

    def preproses_gdd_columns(self, df: dd) -> dd:
        pass

    def preproses_total_rainfall_columns(self, df: dd) -> dd:
        pass

    def calculate_total_gdd(self, df: dd) -> dd:
        pass

    def clean_max_min_temperature_before_today(self, df: dd) -> dd:
        pass

    def calculate_temperature(self, df: dd) -> dd:
        pass

    def calculate_humidity(self, df: dd) -> dd:
        pass
    
    def calculate_p_pet(self, df: dd) -> dd:
        pass

    def calculate_seasonal_precipitation(self, df:dd) -> dd:
        pass

    def clean_intermediate_columns(self, df: dd) -> dd:
        pass

    def calculate_growth_stage(self, df: dd) -> dd:
        pass

    def clean_total_rainfall_before_growth_start_date(self, df: dd) -> dd:
        pass

    def calculate_growth_stage_precipitation(self, df: dd) -> dd:
        pass

    def clean_total_rainfall_intermetediate_columns(self, df: dd) -> dd:
        pass

    def run_rule_engine(self, df: dd) -> dd:
        pass

    def save(self, df: dd):
        df_geo = dg.from_dask_dataframe(
            df,
            geometry=dg.from_wkb(df['geometry'])
        )

        print('Saving to parquet')

        x = to_parquet(
            df_geo,
            self._get_directory_path(),
            partition_on=['iso_a3', 'year', 'month', 'day'],
            filesystem=self.fs,
            compute=False
        )

        print(f'writing to {self._get_directory_path()}')

        execute_dask_compute(x)

    def run(self):
        self.setup()

        grid_df = self.load_grid_data()

        grid_df = self.load_grid_weather_data(grid_df)

        grid_df = self.merge_grid_data_with_crop(grid_df)

        grid_df = self.calculate_gdd(grid_df)

        # print(grid_df)
        # print(grid_df.columns)

        # memory = grid_df.memory_usage(deep=True)
        # total_memory = memory.sum()  # Total memory usage in bytes

        # print(memory)
        # print(f"Total memory usage: {total_memory / 1024:.2f} KB")
        # # 760 MB for 33K grid data

        farm_df = self.load_farm_registry_data()

        farm_df = self.merge_farm_registry_grid_data(farm_df, grid_df)

        self.save(farm_df)
