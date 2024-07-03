# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from django.contrib.gis.geos import Point
from pydap.model import StructureType, BaseType
from pydap.client import open_url

from gap.models.measurement import Attribute
from gap.models.netcdf import (
    NetCDFProviderMetadata,
    NetCDFProviderAttribute,
    NetCDFFile
)


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


def _find_idx_lat_lon(value: float, base_min: float, inc: float, max: int) -> int:
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


def _find_lat_lon_indices(dimensions: tuple[str, ...], idx_lat: int, idx_lon: int):
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


def read_value(netcdf: NetCDFFile, attribute: Attribute, point: Point):
    """Read attribute value from netcdf file for given point.

    :param netcdf: NetCDF File object
    :type netcdf: NetCDFFile
    :param attribute: attribute to be queried
    :type attribute: Attribute
    :param point: Location to be queried
    :type point: Point
    """
    netcdf_metadata = NetCDFProviderMetadata.objects.get(
        provider=netcdf.provider
    )
    metadata = netcdf_metadata.metadata
    attribute_name = NetCDFProviderAttribute.objects.get(
        provider=netcdf.provider,
        attribute=attribute
    ).variable_name
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
    attr_data = dataset[attribute_name]
    return _get_attrib_value(attr_data, attribute_name, idx_lat, idx_lon)
