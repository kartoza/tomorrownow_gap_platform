# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from django.contrib.gis.geos import Point
from pydap.model import StructureType
from pydap.client import open_url

from gap.models.measurement import Attribute
from gap.models.netcdf import (
    NetCDFProviderMetadata,
    NetCDFFile
)


def _find_dimension(attr_data, attr_name: str) -> tuple[str, ...]:
    """_summary_

    :param attr_data: _description_
    :type attr_data: _type_
    :param attr_name: _description_
    :type attr_name: str
    :return: _description_
    :rtype: tuple[str, ...]
    """
    attrib = attr_data
    if isinstance(attr_data, StructureType):
        attrib = attr_data[attr_name]
    return attrib.dimensions


def _find_idx_lat_lon(value: float, base_min: float, inc: float, max: int) -> int:
    """_summary_

    :param value: _description_
    :type value: float
    :param base_min: _description_
    :type base_min: float
    :param inc: _description_
    :type inc: float
    :param max: _description_
    :type max: int
    :return: _description_
    :rtype: int
    """
    if value < base_min:
        return 0
    idx = round((value - base_min) / inc)
    return max - 1 if idx > max else idx


def _slice_along_axes(array, indices):
    """_summary_

    :param array: _description_
    :type array: _type_
    :param indices: _description_
    :type indices: _type_
    :return: _description_
    :rtype: _type_
    """
    slices = [slice(None)] * array.ndim  # Create a list of slices

    # Set the slices for the specified axes
    for axis, index in indices.items():
        slices[axis] = index

    return array[tuple(slices)]


def _find_lat_lon_indices(dimensions: tuple[str, ...], idx_lat: int, idx_lon: int):
    """_summary_

    :param dimensions: _description_
    :type dimensions: tuple[str, ...]
    :param idx_lat: _description_
    :type idx_lat: int
    :param idx_lon: _description_
    :type idx_lon: int
    :return: _description_
    :rtype: _type_
    """
    axe_lat = dimensions.index('lat')
    axe_lon = dimensions.index('lon')
    return {
        axe_lat: idx_lat,
        axe_lon: idx_lon
    }


def _get_attrib_value(attr_data, attr_name: str, idx_lat: int, idx_lon: int):
    """_summary_

    :param attr_data: _description_
    :type attr_data: _type_
    :param attr_name: _description_
    :type attr_name: str
    :param idx_lat: _description_
    :type idx_lat: int
    :param idx_lon: _description_
    :type idx_lon: int
    :return: _description_
    :rtype: _type_
    """
    dimensions = _find_dimension(attr_data, attr_name)
    indices = _find_lat_lon_indices(dimensions, idx_lat, idx_lon)
    return _slice_along_axes(attr_data[attr_name], indices)


def read_value(netcdf: NetCDFFile, attribute: Attribute, point: Point):
    """_summary_

    :param netcdf: _description_
    :type netcdf: NetCDFFile
    :param attribute: _description_
    :type attribute: Attribute
    :param point: _description_
    :type point: Point
    """
    netcdf_metadata = NetCDFProviderMetadata.objects.get(
        provider=netcdf.provider
    )
    metadata = netcdf_metadata.metadata
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
    attr_data = dataset[attribute.name]
    return _get_attrib_value(attr_data, attribute.name, idx_lat, idx_lon)
