# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tio Short Tem ingestor.
"""

import json
import logging
import os
import traceback
import uuid
import zipfile
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da
import geohash
from typing import List
from datetime import timedelta, date

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.contrib.gis.db.models.functions import Centroid
from django.utils import timezone

from core.utils.s3 import zip_folder_in_s3
from gap.ingestor.base import BaseIngestor, BaseZarrIngestor
from gap.ingestor.exceptions import (
    MissingCollectorSessionException, FileNotFoundException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    CastType, CollectorSession, DataSourceFile, DatasetStore, Grid,
    IngestorSession, Dataset
)
from gap.providers import TomorrowIODatasetReader
from gap.providers.tio import tomorrowio_shortterm_forecast_dataset
from gap.utils.reader import DatasetReaderInput
from gap.utils.zarr import BaseZarrReader
from gap.utils.netcdf import find_start_latlng
from gap.utils.dask import execute_dask_compute


logger = logging.getLogger(__name__)


def path(filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}tio-short-term-collector/{filename}'


class TioShortTermCollector(BaseIngestor):
    """Collector for Tio Short Term data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize TioShortTermCollector."""
        super().__init__(session, working_dir)
        self.dataset = tomorrowio_shortterm_forecast_dataset()
        today = timezone.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Retrieve D-6 to D+14
        # Total days: 21
        self.start_dt = today - timedelta(days=6)
        self.end_dt = today + timedelta(days=15)
        self.forecast_date = today

    def _run(self):
        """Run TomorrowIO ingestor."""
        s3_storage = default_storage
        zip_file = path(f"{uuid.uuid4()}.zip")
        dataset = self.dataset
        start_dt = self.start_dt
        end_dt = self.end_dt
        data_source_file, _ = DataSourceFile.objects.get_or_create(
            dataset=dataset,
            start_date_time=start_dt,
            end_date_time=end_dt,
            format=DatasetStore.ZIP_FILE,
            defaults={
                'name': zip_file,
                'created_on': timezone.now(),
                'metadata': {
                    'forecast_date': self.forecast_date.date().isoformat()
                }
            }
        )
        filename = data_source_file.name.split('/')[-1]
        _uuid = os.path.splitext(filename)[0]
        zip_file = path(f"{_uuid}.zip")
        folder = path(_uuid)

        # If it is already have zip file, skip the process
        if s3_storage.exists(zip_file):
            return

        TomorrowIODatasetReader.init_provider()
        for grid in Grid.objects.all():
            file_name = f"grid-{grid.id}.json"
            bbox_filename = os.path.join(folder, file_name)

            # If the json file is exist, skip it
            if s3_storage.exists(bbox_filename):
                continue

            # Get the data
            location_input = DatasetReaderInput.from_polygon(
                grid.geometry
            )
            forecast_attrs = dataset.datasetattribute_set.filter(
                dataset__type__type=CastType.FORECAST
            )
            reader = TomorrowIODatasetReader(
                dataset,
                forecast_attrs,
                location_input, start_dt, end_dt
            )
            reader.read()
            values = reader.get_data_values()

            # Save the reasult to file
            content = ContentFile(
                json.dumps(values.to_json(), separators=(',', ':')))
            s3_storage.save(bbox_filename, content)

        # Zip the folder
        zip_folder_in_s3(
            s3_storage, folder_path=folder, zip_file_name=zip_file
        )

        # Add data source file to collector result
        self.session.dataset_files.set([data_source_file])

    def run(self):
        """Run Tio Short Term Ingestor."""
        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            logger.error('Ingestor Tio Short Term failed!', e)
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass


class CoordMapping:
    """Mapping coordinate between Grid and Zarr."""

    def __init__(self, value, nearest_idx, nearest_val) -> None:
        """Initialize coordinate mapping class.

        :param value: lat/lon value from Grid
        :type value: float
        :param nearest_idx: nearest index in Zarr
        :type nearest_idx: int
        :param nearest_val: nearest value in Zarr
        :type nearest_val: float
        """
        self.value = value
        self.nearest_idx = nearest_idx
        self.nearest_val = nearest_val


class TioShortTermIngestor(BaseZarrIngestor):
    """Ingestor Tio Short Term data into Zarr."""

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
        """Initialize TioShortTermIngestor."""
        super().__init__(session, working_dir)

        self.metadata = {
            'chunks': [],
            'total_json_processed': 0
        }

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

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        return Dataset.objects.get(
            name='Tomorrow.io Short-term Forecast',
            store_type=DatasetStore.ZARR
        )

    def _is_date_in_zarr(self, date: date) -> bool:
        """Check whether a date has been added to zarr file.

        :param date: date to check
        :type date: date
        :return: True if date exists in zarr file.
        :rtype: bool
        """
        if self.created:
            return False
        if self.existing_dates is None:
            ds = self._open_zarr_dataset(self.variables)
            self.existing_dates = ds.forecast_date.values
            ds.close()
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in self.existing_dates

    def _append_new_forecast_date(
            self, forecast_date: date, is_new_dataset=False):
        """Append a new forecast date to the zarr structure.

        The dataset will be initialized with empty values.
        :param forecast_date: forecast date
        :type forecast_date: date
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
            x = ds.to_zarr(
                zarr_url, mode='w', consolidated=True,
                encoding=encoding,
                storage_options=self.s3_options,
                compute=False
            )
        else:
            # append
            x = ds.to_zarr(
                zarr_url, mode='a', append_dim='forecast_date',
                consolidated=True,
                storage_options=self.s3_options,
                compute=False
            )
        execute_dask_compute(x)

        # close dataset and remove empty_data
        ds.close()
        del ds
        del empty_data

    def _is_sorted_and_incremented(self, arr):
        """Check if array is sorted ascending and incremented by 1.

        :param arr: array
        :type arr: List
        :return: True if array is sorted and incremented by 1
        :rtype: bool
        """
        if not arr:
            return False
        if len(arr) == 1:
            return True
        return all(arr[i] + 1 == arr[i + 1] for i in range(len(arr) - 1))

    def _transform_coordinates_array(
            self, coord_arr, coord_type) -> List[CoordMapping]:
        """Find nearest in Zarr for array of lat/lon.

        :param coord_arr: array of lat/lon
        :type coord_arr: List[float]
        :param coord_type: lat or lon
        :type coord_type: str
        :return: List CoordMapping with nearest val/idx
        :rtype: List[CoordMapping]
        """
        # open existing zarr
        ds = self._open_zarr_dataset()

        # find nearest coordinate for each item
        results: List[CoordMapping] = []
        for target_coord in coord_arr:
            if coord_type == 'lat':
                nearest_coord = ds['lat'].sel(
                    lat=target_coord, method='nearest',
                    tolerance=self.reindex_tolerance
                ).item()
            else:
                nearest_coord = ds['lon'].sel(
                    lon=target_coord, method='nearest',
                    tolerance=self.reindex_tolerance
                ).item()

            coord_idx = np.where(ds[coord_type].values == nearest_coord)[0][0]
            results.append(
                CoordMapping(target_coord, coord_idx, nearest_coord)
            )

        # close dataset
        ds.close()

        return results

    def _find_chunk_slices(
            self, arr_length: int, chunk_size: int) -> List:
        """Create chunk slices for processing Tio data.

        Given arr with length 300 and chunk_size 150,
        this method will return [slice(0, 150), slice(150, 300)].
        :param arr_length: length of array
        :type arr_length: int
        :param chunk_size: chunk size
        :type chunk_size: int
        :return: list of slice
        :rtype: List
        """
        coord_slices = []
        for coord_range in range(0, arr_length, chunk_size):
            max_idx = coord_range + chunk_size
            coord_slices.append(
                slice(
                    coord_range,
                    max_idx if max_idx < arr_length else arr_length
                )
            )
        return coord_slices

    def _update_by_region(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], new_data: dict):
        """Update new_data to the zarr by its forecast_date.

        The lat_arr and lon_arr should already be chunked
        before calling this method.
        :param forecast_date: forecast date of the new data
        :type forecast_date: date
        :param lat_arr: list of lat coordinate mapping
        :type lat_arr: List[CoordMapping]
        :param lon_arr: list of lon coordinate mapping
        :type lon_arr: List[CoordMapping]
        :param new_data: dictionary of new data
        :type new_data: dict
        """
        # open existing zarr
        ds = self._open_zarr_dataset()

        # find index of forecast_date
        forecast_date_array = pd.date_range(
            forecast_date.isoformat(), periods=1)
        new_forecast_date = forecast_date_array[0]
        forecast_date_idx = (
            np.where(ds['forecast_date'].values == new_forecast_date)[0][0]
        )

        # find nearest lat and lon and its indices
        nearest_lat_arr = [lat.nearest_val for lat in lat_arr]
        nearest_lat_indices = [lat.nearest_idx for lat in lat_arr]

        nearest_lon_arr = [lon.nearest_val for lon in lon_arr]
        nearest_lon_indices = [lon.nearest_idx for lon in lon_arr]

        # ensure that the lat/lon indices are in correct order
        assert self._is_sorted_and_incremented(nearest_lat_indices)
        assert self._is_sorted_and_incremented(nearest_lon_indices)

        # Create the dataset with updated data for the region
        data_vars = {
            var: (
                ['forecast_date', 'forecast_day_idx', 'lat', 'lon'],
                new_data[var]
            ) for var in new_data
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

        # write the updated data to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        x = new_ds.to_zarr(
            zarr_url,
            mode='a',
            region={
                'forecast_date': slice(
                    forecast_date_idx, forecast_date_idx + 1),
                'forecast_day_idx': slice(None),
                'lat': slice(
                    nearest_lat_indices[0], nearest_lat_indices[-1] + 1),
                'lon': slice(
                    nearest_lon_indices[0], nearest_lon_indices[-1] + 1)
            },
            storage_options=self.s3_options,
            consolidated=True,
            compute=False
        )
        execute_dask_compute(x)

    def _run(self):
        """Process the tio shortterm data into Zarr."""
        collector = self.session.collectors.first()
        if not collector:
            raise MissingCollectorSessionException(self.session.id)
        data_source = collector.dataset_files.first()
        if not data_source:
            raise FileNotFoundException()

        # find forecast date
        if 'forecast_date' not in data_source.metadata:
            raise AdditionalConfigNotFoundException('metadata.forecast_date')
        self.metadata['forecast_date'] = data_source.metadata['forecast_date']
        forecast_date = date.fromisoformat(
            data_source.metadata['forecast_date'])
        if not self._is_date_in_zarr(forecast_date):
            self._append_new_forecast_date(forecast_date, self.created)

        # get lat and lon array from grids
        lat_arr = set()
        lon_arr = set()
        grid_dict = {}

        # query grids
        grids = Grid.objects.annotate(
            centroid=Centroid('geometry')
        )
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
        assert self._is_sorted_and_incremented(lat_indices)
        assert self._is_sorted_and_incremented(lon_indices)

        # create slices for chunks
        lat_slices = self._find_chunk_slices(
            len(lat_arr), self.default_chunks['lat'])
        lon_slices = self._find_chunk_slices(
            len(lon_arr), self.default_chunks['lon'])

        # open zip file and process the data by chunks
        with default_storage.open(data_source.name) as _file:
            with zipfile.ZipFile(_file, 'r') as zip_file:
                for lat_slice in lat_slices:
                    for lon_slice in lon_slices:
                        lat_chunks = lat_arr[lat_slice]
                        lon_chunks = lon_arr[lon_slice]
                        warnings, count = self._process_tio_shortterm_data(
                            forecast_date, lat_chunks, lon_chunks,
                            grid_dict, zip_file
                        )
                        self.metadata['chunks'].append({
                            'lat_slice': str(lat_slice),
                            'lon_slice': str(lon_slice),
                            'warnings': warnings
                        })
                        self.metadata['total_json_processed'] += count

        # update end date of zarr datasource file
        self._update_zarr_source_file(forecast_date)

        # remove temporary source file
        remove_temp_file = self.get_config('remove_temp_file', True)
        if remove_temp_file:
            self._remove_temporary_source_file(data_source, data_source.name)

        # invalidate zarr cache
        self._invalidate_zarr_cache()

    def run(self):
        """Run TomorrowIO Ingestor."""
        # Run the ingestion
        try:
            self._run()
            self.session.notes = json.dumps(self.metadata, default=str)
        except Exception as e:
            logger.error('Ingestor TomorrowIO failed!')
            logger.error(traceback.format_exc())
            raise e
        finally:
            pass

    def _process_tio_shortterm_data(
            self, forecast_date: date, lat_arr: List[CoordMapping],
            lon_arr: List[CoordMapping], grids: dict,
            zip_file: zipfile.ZipFile) -> dict:
        """Process Tio data and update into zarr.

        :param forecast_date: forecast date
        :type forecast_date: date
        :param lat_arr: list of latitude
        :type lat_arr: List[CoordMapping]
        :param lon_arr: list of longitude
        :type lon_arr: List[CoordMapping]
        :param grids: dictionary for geohash and grid id
        :type grids: dict
        :param zip_file: zip file from collector
        :type zip_file: zipfile.ZipFile
        :return: dictionary of warnings
        :rtype: dict
        """
        zip_file_list = zip_file.namelist()
        count = 0
        data_shape = (
            1,
            self.default_chunks['forecast_day_idx'],
            len(lat_arr),
            len(lon_arr)
        )
        warnings = {
            'missing_hash': 0,
            'missing_json': 0,
            'invalid_json': 0
        }

        # initialize empty new data for each variable
        new_data = {}
        for variable in self.variables:
            new_data[variable] = np.empty(data_shape)

        for idx_lat, lat in enumerate(lat_arr):
            for idx_lon, lon in enumerate(lon_arr):
                # find grid id by geohash of lat and lon
                grid_hash = geohash.encode(lat.value, lon.value, precision=8)
                if grid_hash not in grids:
                    warnings['missing_hash'] += 1
                    continue

                # open the grid json file using grid id from grid_hash
                json_filename = f'grid-{grids[grid_hash]}.json'
                if json_filename not in zip_file_list:
                    warnings['missing_json'] += 1
                    continue

                with zip_file.open(json_filename) as _file:
                    data = json.loads(_file.read().decode('utf-8'))

                # there might be invalid json (e.g. API returns error)
                if 'data' not in data:
                    warnings['invalid_json'] += 1
                    continue

                # iterate for each item in data
                assert (
                    len(data['data']) ==
                    self.default_chunks['forecast_day_idx']
                )
                forecast_day_idx = 0
                for item in data['data']:
                    values = item['values']
                    for var in values:
                        if var not in new_data:
                            continue
                        # assign the variable value into new data
                        new_data[var][
                            0, forecast_day_idx, idx_lat, idx_lon] = (
                                values[var]
                        )
                    forecast_day_idx += 1
                count += 1

        # update new data to zarr using region
        self._update_by_region(forecast_date, lat_arr, lon_arr, new_data)
        del new_data

        return warnings, count
