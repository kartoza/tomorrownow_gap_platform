# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

import os
from typing import List
from datetime import datetime, timedelta
from django.contrib.gis.geos import Point
import numpy as np
import xarray as xr
from xarray.core.dataset import Dataset as xrDataset
import fsspec

from gap.models import (
    Provider,
    Dataset,
    DatasetAttribute,
    NetCDFFile
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


class DatasetTimelineValue:
    """Class representing data value for given datetime."""

    def __init__(self, datetime: np.datetime64, values: dict) -> None:
        """Initialize DatasetTimelineValue object.

        :param datetime: datetime of data
        :type datetime: np.datetime64
        :param values: Dictionary of variable and its value
        :type values: dict
        """
        self.datetime = datetime
        self.values = values

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of datetime and values
        :rtype: dict
        """
        return {
            'datetime': np.datetime_as_string(self.datetime, timezone='UTC'),
            'values': self.values
        }


class DatasetReaderValue:
    """Class representing all values from reader."""

    def __init__(
            self, metadata: dict,
            results: List[DatasetTimelineValue]) -> None:
        """Initialize DatasetReaderValue object.

        :param metadata: Dictionary of metadata
        :type metadata: dict
        :param results: Data value list
        :type results: List[DatasetTimelineValue]
        """
        self.metadata = metadata
        self.results = results

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        return {
            'metadata': self.metadata,
            'data': [result.to_dict() for result in self.results]
        }


class BaseNetCDFReader:
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
        self.dataset = dataset
        self.attributes = attributes
        self.point = point
        self.start_date = start_date
        self.end_date = end_date
        self.xrDatasets = []

    def add_attribute(self, attribute: DatasetAttribute):
        """Add a new attribuute to be read.

        :param attribute: Dataset Attribute
        :type attribute: DatasetAttribute
        """
        is_existing = [a for a in self.attributes if a.id == attribute.id]
        if len(is_existing) == 0:
            self.attributes.append(attribute)

    def get_attributes_metadata(self) -> dict:
        """Get attributes metadata (unit and desc).

        :return: Dictionary of attribute and its metadata
        :rtype: dict
        """
        results = {}
        for attrib in self.attributes:
            results[attrib.attribute.variable_name] = {
                'units': attrib.attribute.unit.name,
                'longname': attrib.attribute.name
            }
        return results

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

    def read_variables(self, dataset: xrDataset) -> xrDataset:
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

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        pass

    def read_historical_data(self):
        """Read historical data from dataset."""
        pass

    def read_forecast_data(self):
        """Read forecast data from dataset."""
        pass

    @classmethod
    def from_dataset(cls, dataset: Dataset):
        """Create a new Reader from given dataset.

        :param dataset: Dataset to be read
        :type dataset: Dataset
        :raises TypeError: if provider is neither CBAM or Salient
        :return: Reader Class Type
        :rtype: CBAMNetCDFReader|SalientNetCDFReader
        """
        if dataset.provider.name == NetCDFProvider.CBAM:
            return CBAMNetCDFReader
        elif dataset.provider.name == NetCDFProvider.SALIENT:
            return SalientNetCDFReader
        else:
            raise TypeError(
                f'Unsupported provider name: {dataset.provider.name}'
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

    def read_historical_data(self):
        """Read historical data from dataset."""
        self.setup_netcdf_reader()
        self.xrDatasets = []
        for filter_date in daterange_inc(self.start_date, self.end_date):
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

    def read_forecast_data(self):
        """Read forecast data from dataset."""
        raise NotImplementedError(
            'CBAM does not have forecast data implementation!')

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from list of xArray Dataset object.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        results = []
        val = xr.combine_nested(
            self.xrDatasets, concat_dim=[self.date_variable])
        for dt_idx, dt in enumerate(val[self.date_variable].values):
            value_data = {}
            for attribute in self.attributes:
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
