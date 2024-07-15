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

from gap.models import (
    Dataset,
    DatasetAttribute,
    NetCDFFile
)

from gap.utils.reader import DatasetTimelineValue, DatasetReaderValue
from gap.utils.netcdf import (
    daterange_inc,
    BaseNetCDFReader
)



class CBAMNetCDFReader(BaseNetCDFReader):
    """Class to read NetCDF file from CBAM provider."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            point: Point, start_date: datetime, end_date: datetime) -> None:
        """Initialize CBAMNetCDFReader class.

        :param dataset: Dataset from CBAM provider
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
            netcdf_file = NetCDFFile.objects.filter(
                dataset=self.dataset,
                start_date_time__gte=filter_date,
                end_date_time__lte=filter_date
            ).first()
            if netcdf_file is None:
                continue
            ds = self.open_dataset(netcdf_file)
            val = self.read_variables(ds)
            self.xrDatasets.append(val)

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
        val = xr.combine_nested(
            self.xrDatasets, concat_dim=[self.date_variable])
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
        return DatasetReaderValue(metadata, results)
