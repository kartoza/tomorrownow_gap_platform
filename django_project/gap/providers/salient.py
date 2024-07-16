# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Data Reader
"""

import json
from typing import List
from datetime import datetime
from django.contrib.gis.geos import Point
import numpy as np
import regionmask
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
from shapely.geometry import shape

from gap.models import (
    Dataset,
    DatasetAttribute,
    NetCDFFile
)

from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    LocationDatasetReaderValue
)
from gap.utils.netcdf import (
    BaseNetCDFReader
)


class SalientNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from Salient provider."""

    date_variable = 'forecast_day'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize CBAMNetCDFReader class.

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
        self.setup_netcdf_reader()
        self.xrDatasets = []
        netcdf_file = NetCDFFile.objects.filter(
            dataset=self.dataset
        ).order_by('id').last()
        if netcdf_file is None:
            return
        ds = self.open_dataset(netcdf_file)
        val = self.read_variables(ds, start_date, end_date)
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
        shapely_multipolygon = shape(json.loads(self.location_input.polygon.geojson))

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
        var_dims = dataset[variables[0]].dims
        # use the first variable to get its dimension
        if 'ensemble' in var_dims:
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
        mask_da = xr.DataArray(mask, coords={'lat': dataset['lat'], 'lon': dataset['lon']}, dims=['lat', 'lon'])
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
