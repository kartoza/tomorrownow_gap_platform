# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Data Reader
"""

from typing import List
from datetime import datetime
from django.contrib.gis.geos import Point
import numpy as np
from xarray.core.dataset import Dataset as xrDataset

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

    def read_variables(
            self, dataset: xrDataset, start_date: datetime = None,
            end_date: datetime = None) -> xrDataset:
        """Read data from list variable with filter from given Point.

        :param dataset: xArray Dataset object
        :type dataset: xrDataset
        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        :return: filtered xArray Dataset object
        :rtype: xrDataset
        """
        start_dt = np.datetime64(start_date)
        end_dt = np.datetime64(end_date)
        variables = [a.source for a in self.attributes]
        variables.append(self.date_variable)
        val = None
        if self.location_input.get_input_type() == LocationInputType.POINT:
            val = dataset[variables].sel(
                lat=self.location_input.points[0].y,
                lon=self.location_input.points[0].x,
                method='nearest'
            ).where(
                (dataset[self.date_variable] >= start_dt) &
                (dataset[self.date_variable] <= end_dt),
                drop=True
            )
        elif self.location_input.get_input_type() == LocationInputType.BBOX:
            lat_min = self.location_input.points[0].y
            lat_max = self.location_input.points[1].y
            lon_min = self.location_input.points[0].x
            lon_max = self.location_input.points[1].x
            val = dataset[variables].where(
                (dataset.lat >= lat_min) & (dataset.lat <= lat_max) &
                (dataset.lon >= lon_min) & (dataset.lon <= lon_max) &
                (dataset[self.date_variable] >= start_dt) &
                (dataset[self.date_variable] <= end_dt),
                drop=True
            )
        return val

    def _get_data_values_from_single_location(
            self, metadata: dict, val: xrDataset) -> DatasetReaderValue:
        """Read data values from xrDataset.

        :param metadata: Dict of metadata
        :type metadata: dict
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
        return DatasetReaderValue(metadata, results)

    def _get_data_values_from_multiple_locations(
            self, metadata: dict, val: xrDataset, locations: List[Point],
            lat_dim: int, lon_dim: int) -> DatasetReaderValue:
        """Read data values from xrDataset from list of locations.

        :param metadata: dict of metadata
        :type metadata: dict
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
                        print(val[attribute.source].dims)
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
        return LocationDatasetReaderValue(metadata, results)

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        results = []
        metadata = {
            'dataset': [self.dataset.name],
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat()
        }
        if len(self.xrDatasets) == 0:
            return DatasetReaderValue(metadata, results)
        # forecast will always use latest dataset
        val = self.xrDatasets[0]
        locations, lat_dim, lon_dim = self.find_locations(val)
        if locations and len(locations) > 1:
            return self._get_data_values_from_multiple_locations(
                metadata, val, locations, lat_dim, lon_dim
            )
        return self._get_data_values_from_single_location(metadata, val)
