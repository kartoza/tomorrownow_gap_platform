# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Inputs
"""

import os
import datetime
import time
import pytz
import pandas as pd
import xarray as xr
import numpy as np
from xarray.core.dataset import Dataset as xrDataset

from gap.models import Dataset, DatasetStore, DatasetAttribute
from gap.providers.tio import TioZarrReader
from gap.utils.reader import DatasetReaderInput
from gap.utils.dask import execute_dask_compute


class DCASPipelineInput:
    """Class to manage data input."""

    TMP_BASE_DIR = '/tmp/dcas'
    NEAREST_TOLERANCE = 0.001

    def __init__(self, request_date):
        """Initialize DCASPipelineInput class."""
        self.dataset = Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )
        self.request_date = request_date
        self.min_plant_date = None
        self.historical_dates = []
        self.historical_epoch = []
        self.forecast_dates = []
        self.precip_dates = []
        self.attribute_maps = {}

    def setup(self, min_plant_date):
        """Set DCASPipelineInput."""
        self.min_plant_date = min_plant_date
        self.historical_dates = pd.date_range(
            self.min_plant_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )

        self.historical_epoch = []
        for date in self.historical_dates:
            epoch = int(date.timestamp())
            self.historical_epoch.append(epoch)

        self.forecast_dates = pd.date_range(
            self.request_date,
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )
        self.precip_dates = pd.date_range(
            self.request_date - datetime.timedelta(days=6),
            self.request_date + datetime.timedelta(days=3),
            freq='d'
        )

        self.attribute_maps = {
            'temperature_and_rainfall': {
                'attribute_list': [
                    'max_temperature',
                    'min_temperature',
                    'total_rainfall'
                ],
                'column_mapping': {
                    'max_temperature': 'max_temperature',
                    'min_temperature': 'min_temperature',
                    'total_rainfall': 'total_rainfall'
                },
                'dates': self.historical_dates,
                'file_path': os.path.join(
                    self.TMP_BASE_DIR,
                    'dcas_input_1.nc'
                )
            },
            'humidity': {
                'attribute_list': [
                    'humidity_maximum',
                    'humidity_minimum'
                ],
                'column_mapping': {
                    'humidity_maximum': 'max_humidity',
                    'humidity_minimum': 'min_humidity'
                },
                'dates': self.forecast_dates,
                'file_path': os.path.join(
                    self.TMP_BASE_DIR,
                    'dcas_input_2.nc'
                )
            },
            'precipitation_and_evapotranspiration': {
                'attribute_list': [
                    'precipitation_probability',
                    'total_evapotranspiration_flux'
                ],
                'column_mapping': {
                    'precipitation_probability': 'precipitation',
                    'total_evapotranspiration_flux': 'evapotranspiration'
                },
                'dates': self.precip_dates,
                'file_path': os.path.join(
                    self.TMP_BASE_DIR,
                    'dcas_input_3.nc'
                )
            }
        }

    def collect_data(self, bbox: list, grid_df: pd.DataFrame) -> pd.DataFrame:
        """Collect data from bbox.

        This will collect below data and store as NetCDF Files:
        - max_temperature, min_temperature, total_rainfall
            from earliest plant date
        - humidity_maximum, humidity_minimum from today to D+3
        - precipitation_probability, total_evapotranspiration_flux
            from D-6 to D+3
        :param bbox: bounding box
        :type bbox: list
        :param grid_df: Dataframe that has lat and lon pairs of Grid
        :type grid_df: pd.DataFrame
        :return: Dataframe with new columns for DCAS data
        :rtype: pd.DataFrame
        """
        for key, data in self.attribute_maps.items():
            start_date = data['dates'][0].to_pydatetime().astimezone(pytz.UTC)
            end_date = data['dates'][-1].to_pydatetime().astimezone(pytz.UTC)
            print(f'Collecting data: {key} from {start_date} to {end_date}')
            self._download_data(
                data['attribute_list'], bbox, start_date, end_date,
                data['file_path']
            )

        # merge to dataframe
        print('Merging dataframe...')
        for key, data in self.attribute_maps.items():
            start_time = time.time()
            grid_df = self.merge_dataset(
                data['file_path'],
                data['attribute_list'],
                data['column_mapping'],
                data['dates'],
                grid_df
            )
            print(f'Merging {key} finished in {time.time() - start_time} s')

        return grid_df

    def merge_dataset(
        self, nc_file_path: str, attribute_list: list, column_mapping: dict,
        dates: list, grid_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge dataset and make columns for each date in collected data.

        If there is no data for specific date, then
        it will be initialized with np.nan.
        :param nc_file_path: File path to the NetCDF file
        :type nc_file_path: str
        :param attribute_list: list of attribute to be read and merged
        :type attribute_list: list
        :param column_mapping: Mapping for renaming columns
        :type column_mapping: dict
        :param dates: list of date to be looked for
        :type dates: list
        :param grid_df: Grid DataFrame
        :type grid_df: pd.DataFrame
        :return: New Dataframe with merged columns
        :rtype: pd.DataFrame
        """
        ds = xr.open_dataset(nc_file_path, engine="h5netcdf")
        result = [grid_df]
        for date in dates:
            vap_df = None
            if date in ds['date'].values:
                vap_df = self._get_values_at_points(
                    ds, attribute_list, column_mapping,
                    grid_df['lat'],
                    grid_df['lon'],
                    date,
                    grid_df.index
                )
            else:
                # initialize default dataframe
                epoch = int(pd.Timestamp(date).timestamp())
                columns = {}
                for col, new_col in column_mapping.items():
                    columns[f'{new_col}_{epoch}'] = np.nan
                vap_df = pd.DataFrame(columns, index=grid_df.index)

            result.append(vap_df)

        merged_df = pd.concat(result, axis=1)

        return merged_df

    def _get_values_at_points(
        self, ds: xrDataset, attribute_list: list, column_mapping: dict,
        lat_arr: pd.Series, lon_arr: pd.Series, date: pd.Timestamp,
        df_index, tolerance: float = None
    ):
        epoch = int(date.timestamp())
        tolerance = self.NEAREST_TOLERANCE if tolerance is None else tolerance

        data = {attribute: [] for attribute in attribute_list}

        for idx, lat in enumerate(lat_arr):
            lon = lon_arr.iloc[idx]

            try:
                values_at_point = ds[attribute_list].sel(
                    date=date
                ).sel(
                    lat=lat,
                    lon=lon,
                    method='nearest',
                    tolerance=tolerance
                )
                for attribute in attribute_list:
                    data[attribute].append(
                        values_at_point[attribute].values.tolist()
                    )
            except KeyError:
                for attribute in attribute_list:
                    data[attribute].append(np.nan)

        vap_df = pd.DataFrame(data, index=df_index)
        # rename columns
        renamed_columns = {}
        for col, new_col in column_mapping.items():
            renamed_columns[col] = f'{new_col}_{epoch}'
        vap_df = vap_df.rename(
            columns=renamed_columns
        )
        return vap_df

    def _download_data(
        self, attribute_list, bbox, start_date, end_date,
        output_file_path
    ):
        attributes = DatasetAttribute.objects.filter(
            attribute__variable_name__in=attribute_list,
            dataset=self.dataset
        )
        location_input = DatasetReaderInput.from_bbox(list(bbox))
        reader = TioZarrReader(
            self.dataset,
            list(attributes),
            location_input,
            start_date,
            end_date
        )

        reader.read()
        reader_value = reader.get_data_values()
        self._write_as_netcdf_file(
            reader_value.xr_dataset, output_file_path
        )

    def _write_as_netcdf_file(self, ds: xrDataset, file_path):
        x = ds.to_netcdf(
            file_path, format='NETCDF4', engine='h5netcdf',
            compute=False
        )
        execute_dask_compute(x)

    def _generate_random(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate random input data."""
        columns = {}
        for epoch in self.historical_epoch:
            columns[f'max_temperature_{epoch}'] = (
                np.random.uniform(low=25, high=40, size=df.shape[0])
            )
            columns[f'min_temperature_{epoch}'] = (
                np.random.uniform(low=5, high=15, size=df.shape[0])
            )
            columns[f'total_rainfall_{epoch}'] = (
                np.random.uniform(low=10, high=200, size=df.shape[0])
            )

        for date in self.forecast_dates:
            epoch = int(date.timestamp())
            columns[f'max_humidity_{epoch}'] = (
                np.random.uniform(low=55, high=90, size=df.shape[0])
            )
            columns[f'min_humidity_{epoch}'] = (
                np.random.uniform(low=10, high=50, size=df.shape[0])
            )

        for date in self.precip_dates:
            epoch = int(date.timestamp())
            columns[f'precipitation_{epoch}'] = (
                np.random.uniform(low=10, high=200, size=df.shape[0])
            )
            columns[f'evapotranspiration_{epoch}'] = (
                np.random.uniform(low=10, high=200, size=df.shape[0])
            )

        new_df = pd.DataFrame(columns, index=df.index)

        return pd.concat([df, new_df], axis=1)
