# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from django.contrib.gis.geos import Point
from pydap.model import StructureType, BaseType
from pydap.client import open_url
import numpy as np
import pandas as pd

from gap.models.measurement import Attribute
from gap.models.netcdf import (
    NetCDFProviderMetadata,
    NetCDFProviderAttribute,
    NetCDFFile
)


class NetCDFProvider:
    """Class contains NetCDF Provider."""

    CBAM = 'CBAM'
    SALIENT = 'Salient'


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
        'Maximum temperature (0000:2300)', 'Deg C'
    ),
    'max_night_temperature': NetCDFVariable(
        'Max Night Temperature',
        'Maximum night-time temperature (1900:0500)', 'Deg C'
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
        'Minimum night-time temperature (1900:0500)', 'Deg C'
    ),
    'max_day_temperature': NetCDFVariable(
        'Max Day Temperature',
        'Maximum day-time temperature (0600:1800)', 'Deg C'
    ),
    'total_rainfall': NetCDFVariable(
        'Total Rainfall',
        'Total rainfall (0000:2300)', 'mm'
    ),
    'min_day_temperature': NetCDFVariable(
        'Min Day Temperature',
        'Minumum day-time temperature (0600:1800)', 'Deg C'
    ),
    'min_total_temperature': NetCDFVariable(
        'Min Total Temperature',
        'Minumum temperature (0000:2300)', 'Deg C'
    ),
}


SALIENT_VARIABLES = {
    'precip_clim': NetCDFVariable(
        'Precipitation Climatology', None, 'mm day-1'
    ),
    'temp_clim': NetCDFVariable(
        'Temperature Climatology', None, 'Deg C'
    ),
    'precip_anom': NetCDFVariable(
        'Precipitation Anomaly', None, 'mm day-1'
    ),
    'temp_anom': NetCDFVariable(
        'Temperature Anomaly', None, 'Deg C'
    ),
    'precip': NetCDFVariable(
        'Precipitation', None, 'mm day-1'
    ),
    'temp': NetCDFVariable(
        'Temperature', None, 'Deg C'
    ),
}


class NetCDFAttributeValue:
    """Base class for parsing value from NetCDF."""

    def __init__(self, attribute: NetCDFProviderAttribute, raw_value) -> None:
        """Initialize the class with attribute and raw_value."""
        self.attribute = attribute
        self.raw_value = raw_value

    @classmethod
    def from_provider(cls, attribute: NetCDFProviderAttribute, raw_value):
        """Create NetCDFAttributeValue based on provider.

        :param attribute: Attribute that has provider
        :type attribute: NetCDFProviderAttribute
        :param raw_value: NetCDF Data
        :type raw_value: any
        :raises TypeError: Raises when provider is not CBAM/Salient
        :return: Returns either CBAMAttributeValue or SalientAttributeValue
        :rtype: NetCDFProviderAttribute
        """
        if attribute.provider.name == 'CBAM':
            return CBAMAttributeValue(attribute, raw_value)
        elif attribute.provider.name == 'Salient':
            return SalientAttributeValue(attribute, raw_value)
        else:
            raise TypeError(
                f'Unsupported provider name: {attribute.provider.name}')

    def get_value(self):
        """Parse the raw value of NetCDF Data."""
        pass


class CBAMAttributeValue(NetCDFAttributeValue):
    """Handle parsing NetCDF Data for CBAM Provider."""

    def __init__(self, attribute: NetCDFProviderAttribute, raw_value) -> None:
        """Initialize CBAMAttributeValue object."""
        super().__init__(attribute, raw_value)

    def get_value(self):
        """Get value from CBAM attribute.

        :return: Attribute value
        :rtype: float
        """
        return self.raw_value[0][0][0].data


class SalientAttributeValue(NetCDFAttributeValue):
    """Handle parsing NetCDF Data for Salient Provider."""

    GRID_ATTRIBUTES = ['precip_clim', 'temp_clim']

    def __init__(self, attribute: NetCDFProviderAttribute, raw_value) -> None:
        """Initialize SalientAttributeValue object."""
        super().__init__(attribute, raw_value)

    def _convert_to_dict(self, array):
        """Convert NumPy array to python dict.

        :param array: NetCDF Data
        :type array: NumPy array
        :return: dictionary representation from NumPy array
        :rtype: dict
        """
        m, n, r = array.shape
        out_arr = np.column_stack(
            (np.repeat(np.arange(m), n), array.reshape(m * n, -1)))
        df = pd.DataFrame(out_arr)
        df = df.to_dict(orient='dict')
        return df[1]

    def _get_grid_value(self):
        """Convert raw_value (GridType).

        :return: dictionary representation from NumPy array
        :rtype: dict
        """
        return self._convert_to_dict(self.raw_value)

    def _get_ensemble_values(self):
        """Convert Ensemble Structure Data.

        :return: Dict<ensemble, Dict<forecast_day, value>>
        :rtype: dict
        """
        # dimension would be ('ensemble', 'forecast_day', 'lat', 'lon')
        ensemble_size = self.raw_value.shape[0]
        results = {}
        for i in range(ensemble_size):
            ensemble_data = self.raw_value[i, :, :, :].data
            results[i] = self._convert_to_dict(ensemble_data)
        return results

    def get_value(self):
        """Get value from Salient attribute.

        precip_clim and temp_clim will return Dict<forecast_day, value>.
        Otherwise, this method will return Dict<ensemble, dict>
        which ensemble is 1-50.

        :return: Attribute value
        :rtype: dict
        """
        if self.attribute.variable_name in self.GRID_ATTRIBUTES:
            return self._get_grid_value()
        return self._get_ensemble_values()


def _find_dimension(attr_data, attr_name: str) -> tuple[str, ...]:
    """Find dimension of data.

    E.g. ('forecast_day', 'lat', 'lon')

    :param attr_data: attribute data
    :type attr_data: PyDap StructureType or GridType
    :param attr_name: attribute name
    :type attr_name: str
    :return: Dimension of data
    :rtype: tuple[str, ...]
    """
    attrib = attr_data
    if isinstance(attr_data, StructureType):
        attrib = attr_data[attr_name]
    return attrib.dimensions


def _find_idx_lat_lon(
        value: float, base_min: float, inc: float, max: int) -> int:
    """Find index of lat/lon value.

    :param value: The value of lat/lon
    :type value: float
    :param base_min: Minimum value of lat/lon
    :type base_min: float
    :param inc: Increment value of lat/lon
    :type inc: float
    :param max: Maximum value of lat/lon
    :type max: int
    :return: Index of lat/lon
    :rtype: int
    """
    if value < base_min:
        return 0
    idx = round((value - base_min) / inc)
    return max - 1 if idx > max else idx


def _slice_along_axes(array, indices):
    """Slice multidimensional array using axes and indices.

    E.g. 3D Array (date, lat, lon) -> array[:, idx_lat, idx_lon]

    :param array: NumPy Array
    :type array: NumPy Array
    :param indices: Key pair of axis and index
    :type indices: dict
    :return: Sliced data
    :rtype: NumPy Array
    """
    slices = [slice(None)] * array.ndim  # Create a list of slices

    # Set the slices for the specified axes
    for axis, index in indices.items():
        slices[axis] = index

    return array[tuple(slices)]


def _find_lat_lon_indices(
        dimensions: tuple[str, ...], idx_lat: int, idx_lon: int):
    """Find lat and lon indices from dimensions tuple.

    :param dimensions: Dimensions of attribute data
    :type dimensions: tuple[str, ...]
    :param idx_lat: index of latitude
    :type idx_lat: int
    :param idx_lon: index of longitude
    :type idx_lon: int
    :return: Key pair of axis and index
    :rtype: dict
    """
    axe_lat = dimensions.index('lat')
    axe_lon = dimensions.index('lon')
    return {
        axe_lat: idx_lat,
        axe_lon: idx_lon
    }


def _get_attrib_value(attr_data, attr_name: str, idx_lat: int, idx_lon: int):
    """Get value of attribute for given lat and lon indices.

    :param attr_data: attribute data from a dataset
    :type attr_data: PyDap StructureType or GridType
    :param attr_name: attribute name
    :type attr_name: str
    :param idx_lat: index of latitude
    :type idx_lat: int
    :param idx_lon: index of longitude
    :type idx_lon: int
    :return: data
    :rtype: Numpy array
    """
    dimensions = _find_dimension(attr_data, attr_name)
    indices = _find_lat_lon_indices(dimensions, idx_lat, idx_lon)
    if isinstance(attr_data, BaseType):
        arr_data = attr_data
    else:
        arr_data = attr_data[attr_name]
    return _slice_along_axes(arr_data, indices)


def read_value(
        netcdf: NetCDFFile, attribute: Attribute,
        point: Point) -> NetCDFAttributeValue:
    """Read attribute value from netcdf file for given point.

    :param netcdf: NetCDF File object
    :type netcdf: NetCDFFile
    :param attribute: attribute to be queried
    :type attribute: Attribute
    :param point: Location to be queried
    :type point: Point
    :return: Attribute value
    :rtype: NetCDFAttributeValue
    """
    netcdf_metadata = NetCDFProviderMetadata.objects.get(
        provider=netcdf.provider
    )
    metadata = netcdf_metadata.metadata
    netcdf_attribute = NetCDFProviderAttribute.objects.get(
        provider=netcdf.provider,
        attribute=attribute
    )
    idx_lat = _find_idx_lat_lon(
        point.y,
        metadata['lat']['min'],
        metadata['lat']['inc'],
        metadata['lat']['size']
    )
    idx_lon = _find_idx_lat_lon(
        point.x,
        metadata['lon']['min'],
        metadata['lon']['inc'],
        metadata['lon']['size']
    )
    dataset = open_url(netcdf.opendap_url)
    attr_data = dataset[netcdf_attribute.variable_name]
    raw_value = _get_attrib_value(
        attr_data, netcdf_attribute.variable_name, idx_lat, idx_lon)
    return NetCDFAttributeValue.from_provider(
        netcdf_attribute, raw_value
    )


def check_netcdf_variables(attribute: Attribute) -> str:
    """Check whether variable belongs to CBAM/Salient Provider.

    :param attribute: attribute object
    :type attribute: Attribute
    :return: CBAM/Salient or None
    :rtype: str
    """
    find_in_cbam = [
        x for x in CBAM_VARIABLES.values() if x.name == attribute.name]
    if len(find_in_cbam) > 0:
        return NetCDFProvider.CBAM
    find_in_salient = [
        x for x in SALIENT_VARIABLES.values() if x.name == attribute.name]
    if len(find_in_salient) > 0:
        return NetCDFProvider.SALIENT
    return None
