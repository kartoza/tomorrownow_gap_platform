# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Data Reader
"""

from typing import List
from datetime import datetime
from django.contrib.gis.geos import Point
import numpy as np
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset

from gap.models import (
    Dataset,
    DatasetAttribute,
    DataSourceFile
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
        self.setup_netcdf_reader()
        self.xrDatasets = []
        for filter_date in daterange_inc(start_date, end_date):
            netcdf_file = DataSourceFile.objects.filter(
                dataset=self.dataset,
                start_date_time__gte=filter_date,
                end_date_time__lte=filter_date
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
