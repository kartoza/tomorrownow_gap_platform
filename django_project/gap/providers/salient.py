# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient Data Reader
"""

import json
from typing import List
from datetime import datetime
from django.contrib.gis.geos import Point
import numpy as np
import regionmask
import xarray as xr
import pandas as pd
from xarray.core.dataset import Dataset as xrDataset
from shapely.geometry import shape

from gap.models import (
    Dataset,
    DatasetAttribute,
    DataSourceFile,
    DatasetStore
)

from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    LocationDatasetReaderValue,
    DatasetReaderValue2
)
from gap.utils.netcdf import (
    BaseNetCDFReader
)
from gap.utils.zarr import BaseZarrReader



class SalientReaderValue(DatasetReaderValue2):
    """Class that convert Salient Dataset to TimelineValues."""

    date_variable = 'forecast_day'

    def __init__(
            self, val: xrDataset | List[DatasetTimelineValue],
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            forecast_date: np.datetime64) -> None:
        self.forecast_date = forecast_date
        super().__init__(val, location_input, attributes)

    def _post_init(self):
        if not self._is_xr_dataset:
            return
        # rename attributes and the forecast_day
        renamed_dict = {
            'forecast_day_idx': 'forecast_day'
        }
        for attr in self.attributes:
            renamed_dict[attr.source] = attr.attribute.variable_name
        self._val = self._val.rename(renamed_dict)

        # replace forecast_day to actualdates
        initial_date = pd.Timestamp(self.forecast_date)
        forecast_day_timedelta = pd.to_timedelta(
            self._val.forecast_day, unit='D')
        forecast_day = initial_date + forecast_day_timedelta
        self._val = self._val.assign_coords(
            forecast_day=('forecast_day', forecast_day))

    def _xr_dataset_to_dict(self) -> dict:
        """Convert xArray Dataset to dictionary.

        Implementation depends on provider.
        :return: data dictionary
        :rtype: dict
        """
        if self.is_empty():
            return {
                'geometry': json.loads(self.location_input.point.json),
                'data': []
            }
        results: List[DatasetTimelineValue] = []
        for dt_idx, dt in enumerate(
            self.xr_dataset[self.date_variable].values):
            value_data = {}
            for attribute in self.attributes:
                var_name = attribute.attribute.variable_name
                if 'ensemble' in self.xr_dataset[var_name].dims:
                    value_data[var_name] = (
                        self.xr_dataset[var_name].values[:, dt_idx]
                    )
                else:
                    v = self.xr_dataset[var_name].values[dt_idx]
                    value_data[var_name] = (
                        v if not np.isnan(v) else None
                    )
            forecast_day = (
                self.xr_dataset.forecast_date.values + np.timedelta64(dt, 'D')
            )
            results.append(DatasetTimelineValue(
                forecast_day,
                value_data
            ))
        return {
            'geometry': json.loads(self.location_input.point.json),
            'data': [result.to_dict() for result in results]
        }


class SalientNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from Salient provider."""

    date_variable = 'forecast_day'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize SalientNetCDFReader class.

        :param dataset: Dataset from Salient provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        self.setup_reader()
        self.xrDatasets = []
        netcdf_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.NETCDF
        ).order_by('id').last()
        if netcdf_file is None:
            return
        ds = self.open_dataset(netcdf_file)
        val = self.read_variables(ds, start_date, end_date)
        if val is None:
            return
        self.xrDatasets.append(val)

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        point = self.location_input.point
        return dataset[variables].sel(
            lat=point.y,
            lon=point.x, method='nearest').where(
                (dataset[self.date_variable] >= start_dt) &
                (dataset[self.date_variable] <= end_dt),
                drop=True
        )

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        points = self.location_input.points
        lat_min = points[0].y
        lat_max = points[1].y
        lon_min = points[0].x
        lon_max = points[1].x
        # output results is in two dimensional array
        return dataset[variables].where(
            (dataset.lat >= lat_min) & (dataset.lat <= lat_max) &
            (dataset.lon >= lon_min) & (dataset.lon <= lon_max) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # Convert the Django GIS Polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)
        # Mask the dataset
        return dataset[variables].where(
            (mask == 0) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # use the first variable to get its dimension
        if self._has_ensembles():
            # use 0 idx ensemble and 0 idx forecast_day
            mask = np.zeros_like(dataset[variables[0]][0][0], dtype=bool)
        else:
            # use the 0 index for it's date variable
            mask = np.zeros_like(dataset[variables[0]][0], dtype=bool)
        # Iterate through the points and update the mask
        for lon, lat in self.location_input.points:
            # Find nearest lat and lon indices
            lat_idx = np.abs(dataset['lat'] - lat).argmin()
            lon_idx = np.abs(dataset['lon'] - lon).argmin()
            mask[lat_idx, lon_idx] = True
        mask_da = xr.DataArray(
            mask,
            coords={
                'lat': dataset['lat'], 'lon': dataset['lon']
            }, dims=['lat', 'lon']
        )
        # Apply the mask to the dataset
        return dataset[variables].where(
            (mask_da) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def _get_data_values_from_single_location(
            self, location: Point, val: xrDataset) -> DatasetReaderValue:
        """Read data values from xrDataset.

        :param location: grid cell from query
        :type location: Point
        :param val: dataset to be read
        :type val: xrDataset
        :return: Data Values
        :rtype: DatasetReaderValue
        """
        results = []
        for dt_idx, dt in enumerate(val[self.date_variable].values):
            value_data = {}
            for attribute in self.attributes:
                if 'ensemble' in val[attribute.source].dims:
                    value_data[attribute.attribute.variable_name] = (
                        val[attribute.source].values[:, dt_idx]
                    )
                else:
                    v = val[attribute.source].values[dt_idx]
                    value_data[attribute.attribute.variable_name] = (
                        v if not np.isnan(v) else None
                    )
            results.append(DatasetTimelineValue(
                dt,
                value_data
            ))
        return DatasetReaderValue(location, results)

    def _get_data_values_from_multiple_locations(
            self, val: xrDataset, locations: List[Point],
            lat_dim: int, lon_dim: int) -> DatasetReaderValue:
        """Read data values from xrDataset from list of locations.

        :param val: dataset to be read
        :type val: xrDataset
        :param locations: list of location
        :type locations: List[Point]
        :param lat_dim: latitude dimension
        :type lat_dim: int
        :param lon_dim: longitude dimension
        :type lon_dim: int
        :return: Data Values
        :rtype: DatasetReaderValue
        """
        results = {}
        for dt_idx, dt in enumerate(val[self.date_variable].values):
            idx_lat_lon = 0
            for idx_lat in range(lat_dim):
                for idx_lon in range(lon_dim):
                    value_data = {}
                    for attribute in self.attributes:
                        if 'ensemble' in val[attribute.source].dims:
                            value_data[attribute.attribute.variable_name] = (
                                val[attribute.source].values[
                                    :, dt_idx, idx_lat, idx_lon]
                            )
                        else:
                            v = val[attribute.source].values[
                                dt_idx, idx_lat, idx_lon]
                            value_data[attribute.attribute.variable_name] = (
                                v if not np.isnan(v) else None
                            )
                    loc = locations[idx_lat_lon]
                    if loc in results:
                        results[loc].append(DatasetTimelineValue(
                            dt,
                            value_data
                        ))
                    else:
                        results[loc] = [DatasetTimelineValue(
                            dt,
                            value_data
                        )]
                    idx_lat_lon += 1
        return LocationDatasetReaderValue(results)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        if len(self.xrDatasets) == 0:
            return DatasetReaderValue(None, [])
        # forecast will always use latest dataset
        val = self.xrDatasets[0]
        locations, lat_dim, lon_dim = self.find_locations(val)
        if self.location_input.type != LocationInputType.POINT:
            return self._get_data_values_from_multiple_locations(
                val, locations, lat_dim, lon_dim
            )
        return self._get_data_values_from_single_location(locations[0], val)


class SalientZarrReader(BaseZarrReader, SalientNetCDFReader):
    """Salient Zarr Reader."""

    date_variable = 'forecast_day_idx'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize SalientZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)
        self.latest_forecast_date = None

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        self.setup_reader()
        self.xrDatasets = []
        zarr_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.ZARR
        ).order_by('id').last()
        if zarr_file is None:
            return
        ds = self.open_dataset(zarr_file)
        # get latest forecast date
        self.latest_forecast_date = ds['forecast_date'][-1].values
        val = self.read_variables(ds, start_date, end_date)
        if val is None:
            return
        self.xrDatasets.append(val)

    def get_data_values2(self) -> DatasetReaderValue2:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        val = None
        if len(self.xrDatasets) > 0:
            val = self.xrDatasets[0]
        return SalientReaderValue(
            val, self.location_input, self.attributes,
            self.latest_forecast_date)

    def _get_forecast_day_idx(self, date: np.datetime64) -> int:
        return int(
            abs((date - self.latest_forecast_date) / np.timedelta64(1, 'D'))
        )

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from single point.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        point = self.location_input.point
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date,
            lat=point.y,
            lon=point.x, method='nearest').where(
                (dataset[self.date_variable] >= min_idx) &
                (dataset[self.date_variable] <= max_idx),
                drop=True
        )

    def _read_variables_by_bbox(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from a bbox.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        points = self.location_input.points
        lat_min = points[0].y
        lat_max = points[1].y
        lon_min = points[0].x
        lon_max = points[1].x
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        # output results is in two dimensional array
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date
        ).where(
            (dataset.lat >= lat_min) & (dataset.lat <= lat_max) &
            (dataset.lon >= lon_min) & (dataset.lon <= lon_max) &
            (dataset[self.date_variable] >= min_idx) &
            (dataset[self.date_variable] <= max_idx),
            drop=True
        )

    def _read_variables_by_polygon(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from a polygon.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        # Convert the Django GIS Polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)
        # Mask the dataset
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date
        ).where(
            (mask == 0) &
            (dataset[self.date_variable] >= min_idx) &
            (dataset[self.date_variable] <= max_idx),
            drop=True
        )

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from a list of point.

        :param dataset: Dataset to be read
        :type dataset: xrDataset
        :param variables: list of variable name
        :type variables: List[str]
        :param start_dt: start datetime
        :type start_dt: np.datetime64
        :param end_dt: end datetime
        :type end_dt: np.datetime64
        :return: Dataset that has been filtered
        :rtype: xrDataset
        """
        min_idx = self._get_forecast_day_idx(start_dt)
        max_idx = self._get_forecast_day_idx(end_dt)
        if self._has_ensembles():
            # use 0 idx ensemble and 0 idx forecast_day
            mask = np.zeros_like(dataset[variables[0]][0][0][0], dtype=bool)
        else:
            # use the 0 index for it's date variable
            mask = np.zeros_like(dataset[variables[0]][0][0], dtype=bool)
        # Iterate through the points and update the mask
        for lon, lat in self.location_input.points:
            # Find nearest lat and lon indices
            lat_idx = np.abs(dataset['lat'] - lat).argmin()
            lon_idx = np.abs(dataset['lon'] - lon).argmin()
            mask[lat_idx, lon_idx] = True
        mask_da = xr.DataArray(
            mask,
            coords={
                'lat': dataset['lat'], 'lon': dataset['lon']
            }, dims=['lat', 'lon']
        )
        # Apply the mask to the dataset
        return dataset[variables].sel(
            forecast_date=self.latest_forecast_date
        ).where(
            (mask_da) &
            (dataset[self.date_variable] >= min_idx) &
            (dataset[self.date_variable] <= max_idx), drop=True)
