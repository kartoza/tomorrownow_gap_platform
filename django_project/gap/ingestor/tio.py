# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient ingestor.
"""

import os
import uuid
import json
import logging
import datetime
import time
import pytz
import traceback
import fsspec
import s3fs
import math
import zipfile
import numpy as np
import pandas as pd
import xarray as xr
import salientsdk as sk
from typing import List
from xarray.core.dataset import Dataset as xrDataset
import dask.array as da
from django.utils import timezone
from django.core.files.storage import default_storage
from django.contrib.gis.db.models.functions import Centroid, GeoHash as GH
from django.db.models import F
import geohash
from memory_profiler import profile
import gc

from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, CollectorSession, Preferences,
    Grid
)
from gap.ingestor.base import BaseIngestor
from gap.utils.netcdf import find_start_latlng
from gap.utils.zarr import BaseZarrReader


logger = logging.getLogger(__name__)


class CoordMapping:

    def __init__(self, value, nearest_idx, nearest_val) -> None:
        self.value = value
        self.nearest_idx = nearest_idx
        self.nearest_val = nearest_val


class TomorrowIOIngestor(BaseIngestor):

    default_chunks = {
        'forecast_date': 10,
        'forecast_day_idx': 21,
        'lat': 150,
        'lon': 110
    }

    variables = [
        'total_rainfall',
        'total_evapotranspiration_flux',
        'max_temperature',
        'min_temperature',
        'precipitation_probability',
        'humidity_maximum',
        'humidity_minimum',
        'wind_speed_avg'
    ]

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize SalientIngestor."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.metadata = {}

        # get zarr data source file
        datasourcefile_id = self.get_config('datasourcefile_id')
        if datasourcefile_id:
            self.datasource_file = DataSourceFile.objects.get(
                id=datasourcefile_id)
            self.created = not self.get_config(
                'datasourcefile_zarr_exists', True)
        else:
            datasourcefile_name = self.get_config(
                'datasourcefile_name', 'salient.zarr')
            self.datasource_file, self.created = (
                DataSourceFile.objects.get_or_create(
                    name=datasourcefile_name,
                    dataset=self.dataset,
                    format=DatasetStore.ZARR,
                    defaults={
                        'created_on': timezone.now(),
                        'start_date_time': timezone.now(),
                        'end_date_time': (
                            timezone.now()
                        )
                    }
                )
            )

        # min+max are the BBOX that GAP processes
        self.lat_metadata = {
            'min': -27,
            'max': 16,
            'inc': 0.03586314,
            'original_min': -4.65013565
        }
        self.lon_metadata = {
            'min': 21.8,
            'max': 52,
            'inc': 0.036353,
            'original_min': 33.91823667
        }
        self.reindex_tolerance = 0.001
        self.existing_dates = None

    def is_date_in_zarr(self, date: datetime.date) -> bool:
        """Check whether a date has been added to zarr file.

        :param date: date to check
        :type date: datetime.date
        :return: True if date exists in zarr file.
        :rtype: bool
        """
        if self.created:
            return False
        if self.existing_dates is None:
            reader = BaseZarrReader(self.dataset, [], None, None, None)
            reader.setup_reader()
            reader.clear_cache(self.datasource_file)
            if not self.datasource_file.metadata:
                self.datasource_file.metadata = {}
            self.datasource_file.metadata['drop_variables'] = (
                self.variables
            )
            ds = reader.open_dataset(self.datasource_file)
            self.existing_dates = ds.forecast_date.values
            self.datasource_file.metadata['drop_variables'] = []
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in self.existing_dates
 
    @profile
    def _append_new_forecast_date(
            self, forecast_date: datetime.date, is_new_dataset=False):
        """Append a new forecast date to the zarr structure.

        The dataset will be initialized with empty values.
        :param forecast_date: forecast date
        :type forecast_date: datetime.date
        """
        # expand lat and lon
        min_lat = find_start_latlng(self.lat_metadata)
        min_lon = find_start_latlng(self.lon_metadata)
        new_lat = np.arange(
            min_lat, self.lat_metadata['max'] + self.lat_metadata['inc'],
            self.lat_metadata['inc']
        )
        new_lon = np.arange(
            min_lon, self.lon_metadata['max'] + self.lon_metadata['inc'],
            self.lon_metadata['inc']
        )

        # create empty data variables
        empty_shape = (
            1,
            self.default_chunks['forecast_day_idx'],
            len(new_lat),
            len(new_lon)
        )
        chunks = (
            1,
            self.default_chunks['forecast_day_idx'],
            self.default_chunks['lat'],
            self.default_chunks['lon']
        )

        # Create the Dataset
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        forecast_day_indices = np.arange(-6, 15, 1)
        data_vars = {}
        encoding = {
            'forecast_date': {
                'chunks': self.default_chunks['forecast_date']
            }
        }
        for var in self.variables:
            empty_data = da.empty(empty_shape, chunks=chunks)
            data_vars[var] = (
                ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
                empty_data
            )
            encoding[var] = {
                'chunks': (
                    self.default_chunks['forecast_date'],
                    self.default_chunks['forecast_day_idx'],
                    self.default_chunks['lat'],
                    self.default_chunks['lon']
                )
            }
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'forecast_date': ('forecast_date', forecast_date_array),
                'forecast_day_idx': (
                    'forecast_day_idx', forecast_day_indices),
                'lat': ('lat', new_lat),
                'lon': ('lon', new_lon)
            }
        )

        # write/append to zarr
        # note: when writing to a new chunk of forecast_date,
        # the memory usage will be higher than the rest
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        if is_new_dataset:
            # write
            ds.to_zarr(
                zarr_url, mode='w', consolidated=True,
                encoding=encoding,
                storage_options=self.s3_options
            )
        else:
            # append
            ds.to_zarr(
                zarr_url, mode='a', append_dim='forecast_date',
                consolidated=True,
                storage_options=self.s3_options
            )

        # close dataset and remove empty_data
        ds.close()
        del ds
        del empty_data

    def _is_sorted_and_incremented(self, arr):
        if not arr:  # Handle empty array case
            return False
        if len(arr) == 1:
            return True
        return all(arr[i] + 1 == arr[i + 1] for i in range(len(arr) - 1))

    def _transform_coordinates_array(self, coord_arr, coord_type) -> List[CoordMapping]:
        # open existing zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        ds = xr.open_zarr(s3_mapper, consolidated=True)

        results: List[CoordMapping] = []
        for target_coord in coord_arr:
            if coord_type == 'lat':
                nearest_coord = ds['lat'].sel(
                    lat=target_coord, method='nearest', tolerance=self.reindex_tolerance
                ).item()
            else:
                nearest_coord = ds['lon'].sel(
                    lon=target_coord, method='nearest', tolerance=self.reindex_tolerance
                ).item()
            coord_idx = np.where(ds[coord_type].values == nearest_coord)[0][0]
            results.append(CoordMapping(target_coord, coord_idx, nearest_coord))

        return results

    def _update_by_region(self, forecast_date: datetime.date, lat_arr: List[CoordMapping], lon_arr: List[CoordMapping], new_data):
        # open existing zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        ds = xr.open_zarr(s3_mapper, consolidated=True)

        # find index of forecast_date
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        new_forecast_date = forecast_date_array[0]
        forecast_date_idx = np.where(ds['forecast_date'].values == new_forecast_date)[0][0]

        # find nearest lat and lon and its indices
        nearest_lat_arr = [lat.nearest_val for lat in lat_arr]
        nearest_lat_indices = [lat.nearest_idx for lat in lat_arr]
 
        nearest_lon_arr = [lon.nearest_val for lon in lon_arr]
        nearest_lon_indices = [lon.nearest_idx for lon in lon_arr]

        # ensure that the lat/lon indices are in correct order
        assert self._is_sorted_and_incremented(nearest_lat_indices) == True
        assert self._is_sorted_and_incremented(nearest_lon_indices) == True

        # Create the dataset with updated data for the region
        data_vars = {
            var: (['forecast_date', 'forecast_day_idx', 'lat', 'lon'], new_data[var]) for var in new_data
        }
        new_ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'forecast_date': [new_forecast_date],
                'forecast_day_idx': ds['forecast_day_idx'],
                'lat': nearest_lat_arr,
                'lon': nearest_lon_arr
            }
        )
        # print(new_ds)

        max_lat_idx = nearest_lat_indices[-1] + 1
        max_lon_idx = nearest_lon_indices[-1] + 1

        print(f'lat {slice(nearest_lat_indices[0], max_lat_idx)}')
        print(f'lon {slice(nearest_lon_indices[0], max_lon_idx)}')
        # write the updated data to zarr
        new_ds.to_zarr(zarr_url, mode='a', region={
                'forecast_date': slice(forecast_date_idx, forecast_date_idx + 1),
                'forecast_day_idx': slice(None),
                'lat': slice(nearest_lat_indices[0], max_lat_idx),
                'lon': slice(nearest_lon_indices[0], max_lon_idx)
            },
            storage_options=self.s3_options,
            consolidated=True
        )

    @profile
    def _run(self):

        # TODO: find the forecast_date from data source file
        forecast_date  = datetime.date(2024, 10, 2)
        if not self.is_date_in_zarr(forecast_date):
            self._append_new_forecast_date(forecast_date, self.created)

        # get lat and lon array from grids
        lat_arr = set()
        lon_arr = set()
        grid_dict = {}

        # query grids
        grids = Grid.objects.annotate(
            centroid=Centroid('geometry')
        )
        start_time = time.time()
        i = 0
        for grid in grids:
            lat = round(grid.centroid.y, 8)
            lon = round(grid.centroid.x, 8)
            grid_hash = geohash.encode(lat, lon, precision=8)
            lat_arr.add(lat)
            lon_arr.add(lon)
            grid_dict[grid_hash] = grid.id

        lat_arr = sorted(lat_arr)
        lon_arr = sorted(lon_arr)

        # transform lat lon arrays
        lat_arr = self._transform_coordinates_array(lat_arr, 'lat')
        lon_arr = self._transform_coordinates_array(lon_arr, 'lon')

        lat_indices = [lat.nearest_idx for lat in lat_arr]
        lon_indices = [lon.nearest_idx for lon in lon_arr]
        assert self._is_sorted_and_incremented(lat_indices) == True
        assert self._is_sorted_and_incremented(lon_indices) == True

        lat_slices = []
        lon_slices = []
        for lat_range in range(0, len(lat_arr), self.default_chunks['lat']):
            max_idx = lat_range + self.default_chunks['lat']
            lat_slices.append(
                slice(lat_range, max_idx if max_idx < len(lat_arr) else len(lat_arr))
            )
        for lon_range in range(0, len(lon_arr), self.default_chunks['lon']):
            max_idx = lon_range + self.default_chunks['lon']
            lon_slices.append(
                slice(lon_range, max_idx if max_idx < len(lon_arr) else len(lon_arr))
            )

        # open zip file
        zip_source_file = DataSourceFile.objects.get(id=23)
        with default_storage.open(zip_source_file.name) as _file:
            with zipfile.ZipFile(_file, 'r') as zip_file:
                for lat_slice in lat_slices:
                    for lon_slice in lon_slices:
                        start_time = time.time()
                        lat_chunks = lat_arr[lat_slice]
                        lon_chunks = lon_arr[lon_slice]
                        print(f'Processing chunks: lat {lat_slice} - lon {lon_slice}')
                        self._process_tio_shortterm_data(forecast_date, lat_chunks, lon_chunks, grid_dict, zip_file)
                        print(f'Total processing time {time.time() - start_time} s')

    def run(self):
        """Run TomorrowIO Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error('Ingestor TomorrowIO failed!', e)
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass

    def _process_tio_shortterm_data(self, forecast_date: datetime.date, lat_arr: List[CoordMapping], lon_arr: List[CoordMapping], grids: dict, zip_file: zipfile.ZipFile):
        print(f'processing: lat {len(lat_arr)} lon {len(lon_arr)}')
        zip_file_list = zip_file.namelist()
        count = 0
        data_shape = (1, self.default_chunks['forecast_day_idx'], len(lat_arr), len(lon_arr))
        print(data_shape)
        warnings = {
            'missing_hash': 0,
            'missing_json': 0,
            'invalid_json': 0
        }
        new_data = {}
        for variable in self.variables:
            new_data[variable] = np.empty(data_shape)

        for idx_lat, lat in enumerate(lat_arr):
            for idx_lon, lon in enumerate(lon_arr):
                grid_hash = geohash.encode(lat.value, lon.value, precision=8)
                if grid_hash not in grids:
                    warnings['missing_hash'] += 1
                    continue

                json_filename = f'grid-{grids[grid_hash]}.json'
                if json_filename not in zip_file_list:
                    warnings['missing_json'] += 1
                    continue

                with zip_file.open(json_filename) as _file:
                    data = json.loads(_file.read().decode('utf-8'))
                
                if 'data' not in data:
                    warnings['invalid_json'] += 1
                    continue

                forecast_day_idx = 0
                for item in data['data']:
                    values = item['values']
                    for var in values:
                        if var not in new_data:
                            continue
                        new_data[var][0, forecast_day_idx, idx_lat, idx_lon] = values[var]
                    forecast_day_idx += 1
                count += 1
                if count % 2000 == 0:
                    print(f'Total processed {count}')
        self._update_by_region(forecast_date, lat_arr, lon_arr, new_data)
        del new_data
        print(f'Total write: {count} grids.')
        print(warnings)
        return warnings

    def verify(self):
        """Verify the resulting zarr file."""
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        self.zarr_ds = xr.open_zarr(s3_mapper, consolidated=True)
        print(self.zarr_ds)
