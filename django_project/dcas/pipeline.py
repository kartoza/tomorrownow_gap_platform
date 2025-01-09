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
from functools import partial
import numpy as np
import pandas as pd
from pprint import pprint
from django.db import connection
from django.db.models import Min
import dask.dataframe as dd
from dask.dataframe.core import DataFrame as dask_df
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, select, distinct, column, cast, extract, func, tuple_
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2.functions import ST_X, ST_Y, ST_Centroid
from geoalchemy2.types import Geometry
import dask_geopandas as dg
import geopandas as gpd
from shapely import from_wkb
from dask_geopandas.io.parquet import to_parquet
import shutil
import duckdb

from gap.utils.dask import execute_dask_compute
from gap.models import FarmRegistryGroup, FarmRegistry, CropGrowthStage, Attribute
from dcas.models import DCASConfig
from dcas.rules.rule_engine import DCASRuleEngine
from dcas.rules.variables import DCASData


# DEBUG
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

logger = logging.getLogger(__name__)
# Enable SQLAlchemy engine logging
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

def read_grid_data(parquet_file_path, column_list, grid_id_list):
    """Read grid data from parquet file."""
    conndb = duckdb.connect()
    query = (
        f"""
        SELECT {','.join(column_list)} FROM read_parquet('{parquet_file_path}')
        WHERE grid_id IN {list(grid_id_list)}
        """
    )
    df = conndb.sql(query).df()
    conndb.close()
    return df

def process_partition_total_gdd(df: pd.DataFrame, parquet_file_path, epoch_list) -> pd.DataFrame:
    grid_column_list = ['grid_id']
    for epoch in epoch_list:
        grid_column_list.append(f'max_temperature_{epoch}')
        grid_column_list.append(f'min_temperature_{epoch}')

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(parquet_file_path, grid_column_list, grid_id_list)

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    # TODO: change from configuration based on crop
    df['gdd_base'] = np.random.random_integers(low=5, high=15, size=df.shape[0])
    df['gdd_cap'] = np.random.random_integers(low=25, high=40, size=df.shape[0])

    # add new column to normalize max and min temperature
    for epoch in epoch_list:
        df[f'max_temperature_{epoch}'] = df[f'max_temperature_{epoch}'].clip(upper=df['gdd_cap'])
        df[f'min_temperature_{epoch}'] = df[f'min_temperature_{epoch}'].clip(lower=df['gdd_base'])

    # calculate gdd foreach day from planting_date
    gdd_cols = []
    for epoch in epoch_list:
        c_name = f'gdd_{epoch}'
        df[c_name] = np.where(
            df['planting_date_epoch'] > epoch,
            np.nan,
            ((df[f'max_temperature_{epoch}'] + df[f'min_temperature_{epoch}']) / 2) - df['gdd_base']
        )
        gdd_cols.append(c_name)

    # calculate total_gdd
    df['total_gdd'] = df[gdd_cols].sum(axis=1)

    # data cleanup
    grid_column_list.remove('grid_id')
    grid_column_list.append('gdd_base')
    grid_column_list.append('gdd_cap')
    df = df.drop(columns=grid_column_list + gdd_cols)

    return df


def process_partition_seasonal_precipitation(df: pd.DataFrame, parquet_file_path, epoch_list):
    grid_column_list = ['grid_id']
    for epoch in epoch_list:
        grid_column_list.append(f'total_rainfall_{epoch}')

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(parquet_file_path, grid_column_list, grid_id_list)

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    # calculate seasonal_precipitation
    grid_column_list.remove('grid_id')
    df['seasonal_precipitation'] = df[grid_column_list].sum(axis=1)

    # data cleanup
    df = df.drop(columns=grid_column_list)

    return df


def process_partition_other_params(df: pd.DataFrame, parquet_file_path) -> pd.DataFrame:
    grid_column_list = ['grid_id', 'temperature', 'humidity', 'p_pet']

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(parquet_file_path, grid_column_list, grid_id_list)

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    return df


def process_partition_growth_stage(df: pd.DataFrame, growth_stage_list, current_date) -> pd.DataFrame:
    df = df.assign(
        growth_stage_start_date=pd.Series(dtype='double'),
        growth_stage_id=pd.Series(dtype='int')
    )

    def calculate_growth_stage(row, gs_list, current_date):
        # TODO: lookup the growth_stage based on total_gdd value
        row['growth_stage_id'] = random.choice(gs_list)
        if (
            row['growth_stage_start_date'] is None or
            pd.isnull(row['prev_growth_stage_id']) or
            pd.isna(row['prev_growth_stage_id']) or
            row['growth_stage_id'] != row['prev_growth_stage_id']
        ):
            row['growth_stage_start_date'] = current_date
        return row
    
    df = df.apply(calculate_growth_stage, axis=1, args=(growth_stage_list, current_date))

    return df


def process_partition_growth_stage_precipitation(df: pd.DataFrame, parquet_file_path, epoch_list) -> pd.DataFrame:
    grid_column_list = ['grid_id']
    for epoch in epoch_list:
        grid_column_list.append(f'total_rainfall_{epoch}')

    # read grid_data_df
    grid_id_list = df.index.unique()
    grid_data_df = read_grid_data(parquet_file_path, grid_column_list, grid_id_list)

    # merge the df with grid_data
    df = df.merge(grid_data_df, on=['grid_id'], how='inner')

    for epoch in epoch_list:
        c_name = f'total_rainfall_{epoch}'
        df[c_name] = np.where(
            df['growth_stage_start_date'] > epoch,
            np.nan,
            df[c_name]
        )

    grid_column_list.remove('grid_id')
    df['growth_stage_precipitation'] = df[grid_column_list].sum(axis=1)

    # data cleanup
    df = df.drop(columns=grid_column_list)

    return df


def process_partition_message_output(df: pd.DataFrame, config_id) -> pd.DataFrame:
    df = df.assign(
        message=None,
        message_2=None,
        message_3=None,
        message_4=None,
        message_5=None
    )

    attrib_dict = {
        'temperature': Attribute.objects.get(variable_name='temperature').id,
        'humidity': Attribute.objects.get(variable_name='relative_humidity').id,
        'p_pet': Attribute.objects.get(variable_name='p_pet').id,
        'growth_stage_precipitation': Attribute.objects.get(variable_name='growth_stage_precipitation').id,
        'seasonal_precipitation': Attribute.objects.get(variable_name='seasonal_precipitation').id
    }

    rule_engine = DCASRuleEngine(DCASConfig.objects.get(id=config_id))
    rule_engine.initialize()

    def calculate_message_output(row, rule_engine):
        params = [
            {
                'id': attribute_id,
                'value': row[key]
            } for key, attribute_id in attrib_dict.items()
        ]
        data = DCASData(
            row['crop_id'],
            row['crop_stage_type_id'],
            row['growth_stage_id'],
            params
        )
        rule_engine.execute_rule(data)

        for idx, code in enumerate(data.message_codes):
            var_name = f'message_{idx + 1}' if idx > 0 else 'message'
            row[var_name] = code
        return row
    
    df = df.apply(calculate_message_output, axis=1, args=(rule_engine,))

    return df


class DCASDataPipeline:
    """Class for DCAS data pipeline."""

    NUM_PARTITIONS = 10
    GRID_CROP_NUM_PARTITIONS = 5 # for 1M 
    # GRID_CROP_NUM_PARTITIONS = 2
    LIMIT = 1000000
    TMP_BASE_DIR = '/tmp/dcas'

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
        # self.rule_engine = DCASRuleEngine(self.config)
        self.base_schema = None
        self.fs = None
        self.minimum_plant_date = None
        self.crops = []
        self.request_date = request_date
        self.max_temperature_epoch = []

    def setup(self):
        """Set the data pipeline."""
        # self.rule_engine.initialize()
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

        # clear temp resource
        if os.path.exists(self.TMP_BASE_DIR):
            shutil.rmtree(self.TMP_BASE_DIR)
        os.makedirs(self.TMP_BASE_DIR)

        self.max_temperature_epoch = []
        dates = pd.date_range(
            self.minimum_plant_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates:
            epoch = int(date.timestamp())
            self.max_temperature_epoch.append(epoch)


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
            self.grid.c.id.label('gdid'),
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
            distinct(column('gdid')),
            ST_Y(column('centroid')).label('lat'),
            ST_X(column('centroid')).label('lon'),
            column('grid_id')
        ).select_from(subquery)

        return pd.read_sql_query(
            sql_query,
            con=self._conn_str(),
            index_col='gdid',
        )

    def load_grid_weather_data(self, df) -> pd.DataFrame:
        columns = {}
        dates = pd.date_range(
            self.minimum_plant_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )

        for date in dates:
            epoch = int(date.timestamp())
            columns[f'max_temperature_{epoch}'] = np.random.uniform(low=25, high=40, size=df.shape[0])
            columns[f'min_temperature_{epoch}'] = np.random.uniform(low=5, high=15, size=df.shape[0])
            columns[f'total_rainfall_{epoch}'] = np.random.uniform(low=10, high=200, size=df.shape[0])

        dates_humidity = pd.date_range(
            self.request_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates_humidity:
            epoch = int(date.timestamp())
            columns[f'max_humidity_{epoch}'] = np.random.uniform(low=55, high=90, size=df.shape[0])
            columns[f'min_humidity_{epoch}'] = np.random.uniform(low=10, high=50, size=df.shape[0])

        dates_precip = pd.date_range(
            self.request_date - datetime.timedelta(days=6),
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates_precip:
            epoch = int(date.timestamp())
            columns[f'precipitation_{epoch}'] = np.random.uniform(low=10, high=200, size=df.shape[0])
            columns[f'evapotranspiration_{epoch}'] = np.random.uniform(low=10, high=200, size=df.shape[0])

        new_df = pd.DataFrame(columns, index=df.index)

        return pd.concat([df, new_df], axis=1)

    def postprocess_grid_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        base_cols = []
        temp_cols = []
        humidity_cols = []
        dates_humidity = pd.date_range(
            self.request_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )

        for date in dates_humidity:
            epoch = int(date.timestamp())
            daily_avg_temp = f'daily_avg_temp_{epoch}'
            temp_cols.append(daily_avg_temp)
            df[daily_avg_temp] = (df[f'max_temperature_{epoch}'] + df[f'min_temperature_{epoch}']) / 2

            daily_avg_humidity = f'daily_avg_humidity_{epoch}'
            humidity_cols.append(daily_avg_humidity)
            df[daily_avg_humidity] = (df[f'max_humidity_{epoch}'] + df[f'min_humidity_{epoch}']) / 2
            
            base_cols.extend([
                f'max_humidity_{epoch}',
                f'min_humidity_{epoch}'
            ])

            # to be confirmed: GDD only from plantingDate to today date? or through 3rd forecast day?
            # if date > self.request_date:
            #     base_cols.extend([
            #         f'max_temperature_{epoch}',
            #         f'min_temperature_{epoch}'
            #     ])

        df['temperature'] = df[temp_cols].mean(axis=1)
        df['humidity'] = df[humidity_cols].mean(axis=1)

        precip_cols = []
        evap_cols = []
        dates_precip = pd.date_range(
            self.request_date - datetime.timedelta(days=6),
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        for date in dates_precip:
            epoch = int(date.timestamp())
            precip_cols.append(f'precipitation_{epoch}')
            evap_cols.append(f'evapotranspiration_{epoch}')
            base_cols.extend([
                f'precipitation_{epoch}',
                f'evapotranspiration_{epoch}'
            ])
        df['total_precipitation'] = df[precip_cols].sum(axis=1)
        df['total_evapotranspiration'] = df[evap_cols].sum(axis=1)
        df['p_pet'] = df['total_precipitation'] / df['total_evapotranspiration']

        df = df.drop(columns=base_cols + temp_cols + humidity_cols)
        return df

    def _get_grid_data_with_crop_query(self):
        subquery = select(
            self.grid.c.id.label('gdid'),
            self.grid.c.id.label('grid_id'),
            self.farmregistry.c.crop_id,
            self.farmregistry.c.crop_stage_type_id,
            self.farmregistry.c.planting_date,
            extract('epoch', func.DATE(self.farmregistry.c.planting_date)).label('planting_date_epoch'),
            self.farmregistry.c.crop_growth_stage_id.label('prev_growth_stage_id'),
            extract('epoch', func.DATE(self.farmregistry.c.growth_stage_start_date)).label('prev_growth_stage_start_date'),
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).where(
            self.farmregistry.c.group_id == self.farm_registry_group.id
        ).order_by(
            self.grid.c.id
        )
        return subquery

    def load_grid_data_with_crop(self) -> dask_df:
        subquery = self._get_grid_data_with_crop_query()        
        if self.LIMIT:
            # for testing purpose
            subquery = subquery.limit(self.LIMIT)
    
        subquery = subquery.subquery('grid_data')
        sql_query = select(
            column('gdid'), column('crop_id'),
            column('crop_stage_type_id'), column('planting_date'),
            column('prev_growth_stage_id'), column('prev_growth_stage_start_date'),
            column('grid_id'), column('planting_date_epoch')
        ).distinct().select_from(subquery)

        ddf = dd.read_sql_query(
            sql=sql_query,
            con=self._conn_str(),
            index_col='gdid',
            npartitions=self.GRID_CROP_NUM_PARTITIONS,
        )

        # adjust type
        ddf = ddf.astype({
            'prev_growth_stage_id': 'Int64',
            'prev_growth_stage_start_date': 'Float64'
        })

        return ddf

    def load_grid_data_with_crop_meta(self) -> pd.DataFrame:
        subquery = self._get_grid_data_with_crop_query()
        subquery = subquery.limit(1)
        subquery = subquery.subquery('grid_data')
        sql_query = select(
            column('gdid'), column('crop_id'),
            column('crop_stage_type_id'), column('planting_date'),
            column('prev_growth_stage_id'), column('prev_growth_stage_start_date'),
            column('grid_id'), column('planting_date_epoch')
        ).distinct().select_from(subquery)
        df = pd.read_sql_query(
            sql_query,
            con=self._conn_str(),
            index_col='gdid',
        )
        df['prev_growth_stage_id'] = df['prev_growth_stage_id'].astype('Int64')
        df['prev_growth_stage_start_date'] = df['prev_growth_stage_start_date'].astype('Float64')
        return df

    def _get_farm_registry_query(self):
        subquery = select(
            self.farmregistry.c.id.label('farmregistry_id'),
            self.farmregistry.c.planting_date.label('planting_date'),
            extract('epoch', func.DATE(self.farmregistry.c.planting_date)).label('planting_date_epoch'),
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

        return subquery

    def load_farm_registry_data(self) -> dask_df:
        subquery = self._get_farm_registry_query()        
        if self.LIMIT:
            # for testing purpose
            subquery = subquery.limit(self.LIMIT)

        subquery = subquery.subquery('farm_data')

        sql_query = select(subquery)

        df = dd.read_sql_query(
            sql=sql_query,
            con=self._conn_str(),
            index_col='farmregistry_id',
            npartitions=self.NUM_PARTITIONS,
        )

        df = df.assign(
            date=pd.Timestamp(self.request_date),
            year=lambda x: x.date.dt.year,
            month=lambda x: x.date.dt.month,
            day=lambda x: x.date.dt.day
        )
        return df

    def load_farm_registry_meta(self) -> pd.DataFrame:
        subquery = self._get_farm_registry_query()   
        subquery = subquery.limit(1)
        subquery = subquery.subquery('farm_data')

        sql_query = select(subquery)
        df = pd.read_sql_query(
            sql_query,
            con=self._conn_str(),
            index_col='farmregistry_id',
        )
    
        df = df.assign(
            date=pd.Timestamp(self.request_date),
            year=lambda x: x.date.dt.year,
            month=lambda x: x.date.dt.month,
            day=lambda x: x.date.dt.day
        )
        return df

    def save(self, df: dask_df, directory_name='test_dcas'):
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

    def save_local_parquet(self, df: dask_df, directory_name):
        dir_path = os.path.join(self.TMP_BASE_DIR, directory_name)
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

    def save_dataframe(self, df: pd.DataFrame, file_name):
        file_path = os.path.join(self.TMP_BASE_DIR, file_name)
        print(f'writing dataframe to {file_path}')
        df.to_parquet(file_path)

    def data_collection(self):
        grid_df = self.load_grid_data()

        grid_df = self.load_grid_weather_data(grid_df)

        grid_df = self.postprocess_grid_weather_data(grid_df)

        # # print(grid_df)
        # print(grid_df.columns)

        # print(f'length grid {grid_df.shape[0]}')
        # memory = grid_df.memory_usage(deep=True)
        # total_memory = memory.sum()  # Total memory usage in bytes

        # print(memory)
        # print(f"Total memory usage: {total_memory / 1024:.2f} KB")
        # # # 760 MB for 33K grid data

        # print(grid_df.columns)
        self.save_dataframe(grid_df, 'base_grid.parquet')
        del grid_df

    def process_grid_crop_data(self):
        request_date_epoch = datetime.datetime(
            self.request_date.year, self.request_date.month, self.request_date.day,
            0, 0, 0
        ).timestamp()
        grid_data_file_path = os.path.join(self.TMP_BASE_DIR, 'base_grid.parquet')

        # load grid with crop and planting date
        grid_crop_df = self.load_grid_data_with_crop()
        grid_crop_df_meta = self.load_grid_data_with_crop_meta()

        # Process total_gdd
        grid_crop_df_meta['total_gdd'] = np.nan
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_total_gdd,
            grid_data_file_path,
            self.max_temperature_epoch,
            meta=grid_crop_df_meta
        )

        # Process seasonal_precipitation
        meta2 = grid_crop_df_meta.assign(
            seasonal_precipitation=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_seasonal_precipitation,
            grid_data_file_path,
            self.max_temperature_epoch,
            meta=meta2
        )

        # Add temperature, humidity, and p_pet
        meta3 = meta2.assign(
            temperature=np.nan,
            humidity=np.nan,
            p_pet=np.nan,
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_other_params,
            grid_data_file_path,
            meta=meta3
        )

        # Identify crop growth stage
        growth_id_list = list(
            CropGrowthStage.objects.all().values_list('id', flat=True)
        )
        meta4 = meta3.assign(
            growth_stage_start_date=pd.Series(dtype='double'),
            growth_stage_id=pd.Series(dtype='int')
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_growth_stage,
            growth_id_list,
            request_date_epoch,
            meta=meta4
        )

        # Calculate growth_stage_precipitation
        meta5 = meta4.assign(
            growth_stage_precipitation=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_growth_stage_precipitation,
            grid_data_file_path,
            self.max_temperature_epoch,
            meta=meta5
        )

        # Calculate message codes
        meta6 = meta5.assign(
            message=None,
            message_2=None,
            message_3=None,
            message_4=None,
            message_5=None
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_message_output,
            self.config.id,
            meta=meta6
        )

        self.save_local_parquet(grid_crop_df, 'grid_crop')

    def run(self):
        self.setup()
        
        self.data_collection()
        self.process_grid_crop_data()

        print('Finished')
