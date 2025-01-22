# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Data Pipeline
"""

import logging
import datetime
import time
import numpy as np
import pandas as pd
from django.core.cache import cache
from django.db import connection
from django.db.models import Min
import dask.dataframe as dd
from dask.dataframe.core import DataFrame as dask_df
from django.contrib.gis.db.models import Union

from gap.models import FarmRegistryGroup, FarmRegistry, Grid
from dcas.service import GrowthStageService
from dcas.models import DCASConfig, DCASConfigCountry
from dcas.partitions import (
    process_partition_total_gdd,
    process_partition_growth_stage,
    process_partition_growth_stage_precipitation,
    process_partition_message_output,
    process_partition_other_params,
    process_partition_seasonal_precipitation
)
from dcas.queries import DataQuery
from dcas.outputs import DCASPipelineOutput, OutputType
from dcas.inputs import DCASPipelineInput


logger = logging.getLogger(__name__)


class DCASDataPipeline:
    """Class for DCAS data pipeline."""

    NUM_PARTITIONS = 10
    # GRID_CROP_NUM_PARTITIONS = 100
    GRID_CROP_NUM_PARTITIONS = 2
    LIMIT = 10000

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
        self.fs = None
        self.minimum_plant_date = None
        self.crops = []
        self.request_date = request_date
        self.data_query = DataQuery(self._conn_str(), self.LIMIT)
        self.data_output = DCASPipelineOutput(request_date)
        self.data_input = DCASPipelineInput(request_date)

    def setup(self):
        """Set the data pipeline."""
        self.data_query.setup()

        # fetch minimum plant date
        self.minimum_plant_date: datetime.date = FarmRegistry.objects.filter(
            group=self.farm_registry_group
        ).aggregate(Min('planting_date'))['planting_date__min']
        # fetch crop id list
        farm_qs = FarmRegistry.objects.filter(
            group=self.farm_registry_group
        ).order_by('crop_id').values_list(
            'crop_id', flat=True
        ).distinct('crop_id')
        self.crops = list(farm_qs)

        # initialize output
        self.data_output.setup()

        # initialize input
        self.data_input.setup(self.minimum_plant_date)

    def _conn_str(self):
        return 'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}'.format(
            **connection.settings_dict
        )

    def cleanup_gdd_matrix(self):
        """Cleanup GDD Matrix."""
        GrowthStageService.cleanup_matrix()

    def load_grid_data(self) -> pd.DataFrame:
        """Load grid data from FarmRegistry table.

        :return: DataFrame of Grid Data
        :rtype: pd.DataFrame
        """
        df = pd.read_sql_query(
            self.data_query.grid_data_query(self.farm_registry_group),
            con=self._conn_str(),
            index_col=self.data_query.grid_id_index_col,
        )

        return self._merge_grid_data_with_config(df)

    def _merge_grid_data_with_config(self, df: pd.DataFrame) -> pd.DataFrame:
        default_config = DCASConfig.objects.filter(
            is_default=True
        ).first()
        config_map = {}
        countries = Grid.objects.filter(
            id__in=df.index.unique()
        ).distinct(
            'country_id'
        ).order_by('country_id').values_list(
            'country_id',
            flat=True
        )
        for country_id in countries:
            if country_id is None:
                continue
            config_for_country = DCASConfigCountry.objects.filter(
                country_id=country_id
            ).first()
            if config_for_country:
                config_map[country_id] = config_for_country.config.id

        if config_map:
            df['config_id'] = df['country_id'].map(
                config_map
            ).fillna(default_config.id)
        else:
            df['config_id'] = default_config.id

        df['config_id'] = df['config_id'].astype('Int64')

        return df

    def load_grid_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Load Grid Weather data.

        TODO: replace with function to read from ZARR/NetCDF File.
        :param df: DataFrame of Grid Data
        :type df: pd.DataFrame
        :return: Grid DataFrame with weather columns
        :rtype: pd.DataFrame
        """
        # Combine all geometries and compute the bounding box
        combined_bbox = (
            Grid.objects.filter(
                id__in=df.index.unique()
            ).aggregate(
                combined_geometry=Union('geometry')
            )
        )

        bbox = combined_bbox['combined_geometry'].extent
        print(f"Bounding Box: {bbox}")

        return self.data_input.collect_data(bbox, df)

    def postprocess_grid_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate value for Grid parameters.

        :param df: Grid DataFrame
        :type df: pd.DataFrame
        :return: DataFrame with column: temperature, humidity,
            total_precipitation, total_evapotranspiration, p_pet
        :rtype: pd.DataFrame
        """
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
            df[daily_avg_temp] = (
                df[f'max_temperature_{epoch}'] +
                df[f'min_temperature_{epoch}']
            ) / 2

            daily_avg_humidity = f'daily_avg_humidity_{epoch}'
            humidity_cols.append(daily_avg_humidity)
            df[daily_avg_humidity] = (
                df[f'max_humidity_{epoch}'] + df[f'min_humidity_{epoch}']
            ) / 2

            base_cols.extend([
                f'max_humidity_{epoch}',
                f'min_humidity_{epoch}'
            ])

            # to be confirmed: GDD only from plantingDate to today date?
            # or through 3rd forecast day?
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
        df['p_pet'] = (
            df['total_precipitation'] / df['total_evapotranspiration']
        )

        df = df.drop(columns=base_cols + temp_cols + humidity_cols)
        return df

    def load_grid_data_with_crop(self) -> dask_df:
        """Load Grid Data distinct with crop and crop_stage_type.

        :return: Dask DataFrame
        :rtype: dask_df
        """
        ddf = dd.read_sql_query(
            sql=self.data_query.grid_data_with_crop_query(
                self.farm_registry_group
            ),
            con=self._conn_str(),
            index_col=self.data_query.grid_id_index_col,
            npartitions=self.GRID_CROP_NUM_PARTITIONS,
        )

        # adjust type
        ddf = ddf.astype({
            'prev_growth_stage_id': 'Int64',
            'prev_growth_stage_start_date': 'Float64'
        })

        return ddf

    def load_farm_registry_data(self) -> dask_df:
        """Load Farm Registry Data.

        :return: Dask DataFrame
        :rtype: dask_df
        """
        sql_query = self.data_query.farm_registry_query(
            self.farm_registry_group
        )

        df = dd.read_sql_query(
            sql=sql_query,
            con=self._conn_str(),
            index_col=self.data_query.farmregistry_id_index_col,
            npartitions=self.NUM_PARTITIONS,
        )

        df = df.assign(
            date=pd.Timestamp(self.request_date),
            year=lambda x: x.date.dt.year,
            month=lambda x: x.date.dt.month,
            day=lambda x: x.date.dt.day
        )
        return df

    def data_collection(self):
        """Run Data Collection step."""
        grid_df = self.load_grid_data()

        grid_df = self.load_grid_weather_data(grid_df)

        grid_df = self.postprocess_grid_weather_data(grid_df)

        self.data_output.save(OutputType.GRID_DATA, grid_df)
        del grid_df

    def process_grid_crop_data(self):
        """Process Grid and Crop Data."""
        # Load GDD Matrix before processing grid crop data
        GrowthStageService.load_matrix()

        grid_data_file_path = self.data_output.grid_data_file_path

        # load grid with crop and planting date
        grid_crop_df = self.load_grid_data_with_crop()
        grid_crop_df_meta = self.data_query.grid_data_with_crop_meta(
            self.farm_registry_group
        )

        # Process gdd cumulative
        # add config_id
        grid_crop_df_meta = grid_crop_df_meta.assign(
            config_id=pd.Series(dtype='Int64')
        )
        gdd_columns = []
        for epoch in self.data_input.historical_epoch:
            grid_crop_df_meta[f'gdd_sum_{epoch}'] = np.nan
            gdd_columns.append(f'gdd_sum_{epoch}')

        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_total_gdd,
            grid_data_file_path,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # Identify crop growth stage
        grid_crop_df_meta = grid_crop_df_meta.assign(
            growth_stage_start_date=pd.Series(dtype='double'),
            growth_stage_id=pd.Series(dtype='int'),
            total_gdd=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_growth_stage,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # drop gdd columns
        grid_crop_df = grid_crop_df.drop(columns=gdd_columns)
        grid_crop_df_meta = grid_crop_df_meta.drop(columns=gdd_columns)

        # Process seasonal_precipitation
        grid_crop_df_meta = grid_crop_df_meta.assign(
            seasonal_precipitation=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_seasonal_precipitation,
            grid_data_file_path,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # Add temperature, humidity, and p_pet
        grid_crop_df_meta = grid_crop_df_meta.assign(
            temperature=np.nan,
            humidity=np.nan,
            p_pet=np.nan,
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_other_params,
            grid_data_file_path,
            meta=grid_crop_df_meta
        )

        # Calculate growth_stage_precipitation
        grid_crop_df_meta = grid_crop_df_meta.assign(
            growth_stage_precipitation=np.nan
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_growth_stage_precipitation,
            grid_data_file_path,
            self.data_input.historical_epoch,
            meta=grid_crop_df_meta
        )

        # Calculate message codes
        grid_crop_df_meta = grid_crop_df_meta.assign(
            message=None,
            message_2=None,
            message_3=None,
            message_4=None,
            message_5=None
        )
        grid_crop_df = grid_crop_df.map_partitions(
            process_partition_message_output,
            meta=grid_crop_df_meta
        )

        return grid_crop_df

    def run(self):
        """Run data pipeline."""
        self.setup()

        start_time = time.time()
        self.data_collection()
        grid_crop_df = self.process_grid_crop_data()

        self.data_output.save(OutputType.GRID_CROP_DATA, grid_crop_df)

        self.cleanup_gdd_matrix()

        print(f'Finished {time.time() - start_time} seconds.')
