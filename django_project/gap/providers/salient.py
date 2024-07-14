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

from gap.utils.reader import DatasetTimelineValue, DatasetReaderValue
from gap.utils.netcdf import (
    BaseNetCDFReader
)


class SalientNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from Salient provider."""

    date_variable = 'forecast_day'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            point: Point, start_date: datetime, end_date: datetime) -> None:
        """Initialize CBAMNetCDFReader class.

        :param dataset: Dataset from Salient provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param point: Location to be queried
        :type point: Point
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(dataset, attributes, point, start_date, end_date)

    def read_historical_data(self):
        """Read historical data from dataset."""
        raise NotImplementedError(
            'Salient does not have historical data implementation!')

    def read_forecast_data(self):
        """Read forecast data from dataset."""
        self.setup_netcdf_reader()
        self.xrDatasets = []
        netcdf_file = NetCDFFile.objects.filter(
            dataset=self.dataset
        ).order_by('id').last()
        if netcdf_file is None:
            return
        ds = self.open_dataset(netcdf_file)
        val = self.read_variables(ds)
        self.xrDatasets.append(val)

    def read_variables(self, dataset: xrDataset) -> xrDataset:
        """Read data from list variable with filter from given Point.

        :param dataset: xArray Dataset object
        :type dataset: xrDataset
        :return: filtered xArray Dataset object
        :rtype: xrDataset
        """
        start_dt = np.datetime64(self.start_date)
        end_dt = np.datetime64(self.end_date)
        variables = [a.source for a in self.attributes]
        variables.append(self.date_variable)
        val = dataset[variables].sel(
            lat=self.point.y, lon=self.point.x,
            method='nearest'
        ).where(
            (dataset[self.date_variable] >= start_dt) &
            (dataset[self.date_variable] <= end_dt),
            drop=True
        )
        return val

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        # forecast will always use latest dataset
        val = self.xrDatasets[0]
        results = []
        for dt_idx, dt in enumerate(val[self.date_variable].values):
            value_data = {}
            for attribute in self.attributes:
                if 'ensemble' in val[attribute.source].dims:
                    value_data[attribute.attribute.variable_name] = (
                        val[attribute.source].values[:, dt_idx]
                    )
                else:
                    value_data[attribute.attribute.variable_name] = (
                        val[attribute.source].values[dt_idx]
                    )
            results.append(DatasetTimelineValue(
                dt,
                value_data
            ))
        metadata = {
            'dataset': [self.dataset.name],
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat()
        }
        return DatasetReaderValue(metadata, results)
