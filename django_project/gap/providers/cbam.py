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
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import regionmask
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
    LocationDatasetReaderValue
)
from gap.utils.netcdf import (
    daterange_inc,
    BaseNetCDFReader
)
from gap.utils.zarr import BaseZarrReader


class CBAMNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from CBAM provider."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize CBAMNetCDFReader class.

        :param dataset: Dataset from CBAM provider
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

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        self.setup_reader()
        self.xrDatasets = []
        for filter_date in daterange_inc(start_date, end_date):
            netcdf_file = DataSourceFile.objects.filter(
                dataset=self.dataset,
                start_date_time__gte=filter_date,
                end_date_time__lte=filter_date,
                format=DatasetStore.NETCDF
            ).first()
            if netcdf_file is None:
                continue
            ds = self.open_dataset(netcdf_file)
            val = self.read_variables(ds, filter_date, filter_date)
            if val is None:
                continue
            self.xrDatasets.append(val)

    def _get_data_values_from_single_location(
            self, point: Point, val: xrDataset) -> DatasetReaderValue:
        """Read data values from xrDataset.

        :param point: grid cell from the query
        :type point: Point
        :param val: dataset to be read
        :type val: xrDataset
        :return: Data Values
        :rtype: DatasetReaderValue
        """
        results = []
        for dt_idx, dt in enumerate(val[self.date_variable].values):
            value_data = {}
            for attribute in self.attributes:
                v = val[attribute.source].values[dt_idx]
                value_data[attribute.attribute.variable_name] = (
                    v if not np.isnan(v) else None
                )
            results.append(DatasetTimelineValue(
                dt,
                value_data
            ))
        return DatasetReaderValue(point, results)

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
        val = xr.combine_nested(
            self.xrDatasets, concat_dim=[self.date_variable])
        locations, lat_dim, lon_dim = self.find_locations(val)
        if self.location_input.type != LocationInputType.POINT:
            return self._get_data_values_from_multiple_locations(
                val, locations, lat_dim, lon_dim
            )
        return self._get_data_values_from_single_location(locations[0], val)

    def _read_variables_by_point(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        point = self.location_input.point
        return dataset[variables].sel(
            lat=point.y,
            lon=point.x, method='nearest')

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
            (dataset.lon >= lon_min) & (dataset.lon <= lon_max), drop=True)

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
        return dataset[variables].where(mask == 0, drop=True)

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        # use the first variable to get its dimension
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
            },
            dims=['lat', 'lon']
        )
        # Apply the mask to the dataset
        return dataset[variables].where(mask_da, drop=True)


class CBAMZarrReader(BaseZarrReader, CBAMNetCDFReader):
    """CBAM Zarr Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize CBAMZarrReader class."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)

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
        """Read variables values from BBOX.

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
        """Read variables values from polygon.

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
        # Convert the Django GIS Polygon to a format compatible with shapely
        shapely_multipolygon = shape(
            json.loads(self.location_input.polygon.geojson))

        # Create a mask using regionmask from the shapely polygon
        mask = regionmask.Regions([shapely_multipolygon]).mask(dataset)
        # TODO: we should pass the non-NA indices from mask to find_locations
        # Mask the dataset
        return dataset[variables].where(
            (mask == 0) &
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt), drop=True)

    def _read_variables_by_points(
            self, dataset: xrDataset, variables: List[str],
            start_dt: np.datetime64,
            end_dt: np.datetime64) -> xrDataset:
        """Read variables values from list of point.

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

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        self.setup_reader()
        zarr_file = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.ZARR
        ).last()
        if zarr_file is None:
            return
        ds = self.open_dataset(zarr_file)
        val = self.read_variables(ds, start_date, end_date)
        if val is None:
            return
        self.xrDatasets.append(val)
