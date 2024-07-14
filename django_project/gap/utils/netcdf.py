# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

import os
from typing import List
from datetime import datetime, timedelta
from django.contrib.gis.geos import Point
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import fsspec

from gap.models import (
    Provider,
    Dataset,
    DatasetAttribute,
    NetCDFFile
)
from gap.utils.reader import (
    BaseDatasetReader
)


class NetCDFProvider:
    """Class contains NetCDF Provider."""

    CBAM = 'CBAM'
    SALIENT = 'Salient'

    @classmethod
    def get_s3_variables(cls, provider: Provider):
        """Get s3 variables for data access.

        :param provider: NetCDF Data Provider
        :type provider: Provider
        :return: Dict<Key, Value> of AWS Credentials
        :rtype: dict
        """
        prefix = provider.name.upper()
        keys = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'AWS_ENDPOINT_URL', 'AWS_BUCKET_NAME',
            'AWS_DIR_PREFIX', 'AWS_REGION_NAME'
        ]
        results = {}
        for key in keys:
            results[key] = os.environ.get(f'{prefix}_{key}', '')
        return results

    @classmethod
    def get_s3_client_kwargs(cls, provider: Provider):
        """Get s3 client_kwargs for s3fs initialization.

        :param provider: NetCDF Data Provider
        :type provider: Provider
        :return: Dict of endpoint_url or region_name
        :rtype: dict
        """
        prefix = provider.name.upper()
        client_kwargs = {}
        if os.environ.get(f'{prefix}_AWS_ENDPOINT_URL', ''):
            client_kwargs['endpoint_url'] = os.environ.get(
                f'{prefix}_AWS_ENDPOINT_URL', '')
        if os.environ.get(f'{prefix}_AWS_REGION_NAME', ''):
            client_kwargs['region_name'] = os.environ.get(
                f'{prefix}_AWS_REGION_NAME', '')
        return client_kwargs


class NetCDFVariable:
    """Contains Variable from NetCDF File."""

    def __init__(self, name, desc, unit=None) -> None:
        """Initialize NetCDFVariable object."""
        self.name = name
        self.desc = desc
        self.unit = unit


CBAM_VARIABLES = {
    'total_evapotranspiration_flux': NetCDFVariable(
        'Total Evapotranspiration Flux',
        'Total Evapotranspiration flux with respect to '
        'grass cover (0000:2300)', 'mm'
    ),
    'max_total_temperature': NetCDFVariable(
        'Max Total Temperature',
        'Maximum temperature (0000:2300)', '°C'
    ),
    'max_night_temperature': NetCDFVariable(
        'Max Night Temperature',
        'Maximum night-time temperature (1900:0500)', '°C'
    ),
    'average_solar_irradiance': NetCDFVariable(
        'Average Solar Irradiance',
        'Average hourly solar irradiance reaching the surface (0600:1800)',
        'MJ/sqm'
    ),
    'total_solar_irradiance': NetCDFVariable(
        'Total Solar Irradiance',
        'Total solar irradiance reaching the surface (0000:2300)', 'MJ/sqm'
    ),
    'min_night_temperature': NetCDFVariable(
        'Min Night Temperature',
        'Minimum night-time temperature (1900:0500)', '°C'
    ),
    'max_day_temperature': NetCDFVariable(
        'Max Day Temperature',
        'Maximum day-time temperature (0600:1800)', '°C'
    ),
    'total_rainfall': NetCDFVariable(
        'Total Rainfall',
        'Total rainfall (0000:2300)', 'mm'
    ),
    'min_day_temperature': NetCDFVariable(
        'Min Day Temperature',
        'Minumum day-time temperature (0600:1800)', '°C'
    ),
    'min_total_temperature': NetCDFVariable(
        'Min Total Temperature',
        'Minumum temperature (0000:2300)', '°C'
    ),
}


SALIENT_VARIABLES = {
    'precip_clim': NetCDFVariable(
        'Precipitation Climatology', None, 'mm day-1'
    ),
    'temp_clim': NetCDFVariable(
        'Temperature Climatology', None, '°C'
    ),
    'precip_anom': NetCDFVariable(
        'Precipitation Anomaly', None, 'mm day-1'
    ),
    'temp_anom': NetCDFVariable(
        'Temperature Anomaly', None, '°C'
    ),
    'precip': NetCDFVariable(
        'Precipitation', None, 'mm day-1'
    ),
    'temp': NetCDFVariable(
        'Temperature', None, '°C'
    ),
}


def daterange_inc(start_date: datetime, end_date: datetime):
    """Iterate through start_date and end_date (inclusive).

    :param start_date: start date
    :type start_date: date
    :param end_date: end date inclusive
    :type end_date: date
    :yield: iteration date
    :rtype: date
    """
    days = int((end_date - start_date).days)
    for n in range(days + 1):
        yield start_date + timedelta(n)


class BaseNetCDFReader(BaseDatasetReader):
    """Base class for NetCDF File Reader."""

    date_variable = 'date'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            point: Point, start_date: datetime, end_date: datetime) -> None:
        """Initialize BaseNetCDFReader class.

        :param dataset: Dataset for reading
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
        self.xrDatasets = []

    def setup_netcdf_reader(self):
        """Initialize s3fs."""
        self.s3 = NetCDFProvider.get_s3_variables(self.dataset.provider)
        self.fs = fsspec.filesystem(
            's3',
            key=self.s3.get('AWS_ACCESS_KEY_ID'),
            secret=self.s3.get('AWS_SECRET_ACCESS_KEY'),
            client_kwargs=(
                NetCDFProvider.get_s3_client_kwargs(self.dataset.provider)
            )
        )

    def open_dataset(self, netcdf_file: NetCDFFile) -> xrDataset:
        """Open a NetCDFFile using xArray.

        :param netcdf_file: NetCDF from a dataset
        :type netcdf_file: NetCDFFile
        :return: xArray Dataset object
        :rtype: xrDataset
        """
        prefix = self.s3['AWS_DIR_PREFIX']
        bucket_name = self.s3['AWS_BUCKET_NAME']
        netcdf_url = f's3://{bucket_name}/{prefix}'
        if not netcdf_url.endswith('/'):
            netcdf_url += '/'
        netcdf_url += f'{netcdf_file.name}'
        return xr.open_dataset(self.fs.open(netcdf_url))

    def read_variables(
            self, dataset: xrDataset, start_date: datetime = None,
            end_date: datetime = None) -> xrDataset:
        """Read data from list variable with filter from given Point.

        :param dataset: xArray Dataset object
        :type dataset: xrDataset
        :return: filtered xArray Dataset object
        :rtype: xrDataset
        """
        variables = [a.source for a in self.attributes]
        variables.append(self.date_variable)
        return dataset[variables].sel(
            lat=self.point.y, lon=self.point.x, method='nearest')
