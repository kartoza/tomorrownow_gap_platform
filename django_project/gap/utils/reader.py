# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading dataset
"""

import json
from typing import Union, List, Dict
import numpy as np
from datetime import datetime
import pytz
import netCDF4 as nc
import tempfile
from xarray.core.dataset import Dataset as xrDataset
from django.contrib.gis.geos import (
    Point, MultiPolygon, GeometryCollection, MultiPoint
)

from gap.models import (
    CastType,
    Attribute,
    Unit,
    Dataset,
    DatasetAttribute
)


class DatasetVariable:
    """Contains Variable from a Dataset."""

    def __init__(
            self, name, desc, unit, attr_var_name=None) -> None:
        """Initialize variable object.

        :param name: Name of the variable
        :type name: str
        :param desc: Description of the variable
        :type desc: str
        :param unit: Unit
        :type unit: str, optional
        :param attr_var_name: Mapping to attribute name, defaults to None
        :type attr_var_name: str, optional
        """
        self.name = name
        self.desc = desc
        self.unit = unit
        self.attr_var_name = attr_var_name

    def get_gap_attribute(self) -> Attribute:
        """Get or create a mapping attribute.

        :return: Gap Attribute
        :rtype: Attribute
        """
        if self.attr_var_name is None:
            return None
        unit, _ = Unit.objects.get_or_create(
            name=self.unit
        )
        attr, _ = Attribute.objects.get_or_create(
            variable_name=self.attr_var_name,
            defaults={
                'description': self.desc,
                'name': self.name,
                'unit': unit,
            }
        )
        return attr


class LocationInputType:
    """Class for data input type."""

    POINT = 'point'
    BBOX = 'bbox'
    POLYGON = 'polygon'
    LIST_OF_POINT = 'list_of_point'


class DatasetReaderInput:
    """Class to store the dataset reader input.

    Input type: Point, bbox, polygon, list of point
    """

    def __init__(self, geom_collection: GeometryCollection, type: str):
        """Initialize DatasetReaderInput class."""
        self.geom_collection = geom_collection
        self.type = type

    @property
    def point(self) -> Point:
        """Get single point from input."""
        if self.type != LocationInputType.POINT:
            raise TypeError('Location input type is not point!')
        return Point(
            x=self.geom_collection[0].x,
            y=self.geom_collection[0].y, srid=4326)

    @property
    def polygon(self) -> MultiPolygon:
        """Get MultiPolygon object from input."""
        if self.type != LocationInputType.POLYGON:
            raise TypeError('Location input type is not polygon!')
        return self.geom_collection

    @property
    def points(self) -> List[Point]:
        """Get list of point from input."""
        if self.type not in [
            LocationInputType.BBOX, LocationInputType.LIST_OF_POINT
        ]:
            raise TypeError('Location input type is not bbox/points!')
        return [
            Point(x=point.x, y=point.y, srid=4326) for
            point in self.geom_collection
        ]

    @classmethod
    def from_point(cls, point: Point):
        """Create input from single point.

        :param point: single point
        :type point: Point
        :return: DatasetReaderInput with POINT type
        :rtype: DatasetReaderInput
        """
        return DatasetReaderInput(
            MultiPoint([point]), LocationInputType.POINT)

    @classmethod
    def from_bbox(cls, bbox_list: List[float]):
        """Create input from bbox (xmin, ymin, xmax, ymax).

        :param bbox_list: (xmin, ymin, xmax, ymax)
        :type bbox_list: List[float]
        :return: DatasetReaderInput with BBOX type
        :rtype: DatasetReaderInput
        """
        return DatasetReaderInput(
            MultiPoint([
                Point(x=bbox_list[0], y=bbox_list[1], srid=4326),
                Point(x=bbox_list[2], y=bbox_list[3], srid=4326)
            ]), LocationInputType.BBOX)


class DatasetReaderOutputType:
    """Dataset Output Type Format."""

    JSON = 'json'
    NETCDF = 'netcdf'
    CSV = 'csv'
    ASCII = 'ascii'


class DatasetTimelineValue:
    """Class representing data value for given datetime."""

    def __init__(
            self, datetime: Union[np.datetime64, datetime],
            values: dict) -> None:
        """Initialize DatasetTimelineValue object.

        :param datetime: datetime of data
        :type datetime: np.datetime64 or datetime
        :param values: Dictionary of variable and its value
        :type values: dict
        """
        self.datetime = datetime
        self.values = values

    def _datetime_as_str(self):
        """Convert datetime object to string."""
        if self.datetime is None:
            return ''
        if isinstance(self.datetime, np.datetime64):
            return np.datetime_as_string(
                self.datetime, unit='s', timezone='UTC')
        return self.datetime.isoformat(timespec='seconds')

    def get_datetime_repr(self, format: str) -> str:
        """Return the representation of datetime in given format.

        :param format: Format like '%Y-%m-%d'
        :type format: str
        :return: String of datetime
        :rtype: str
        """
        dt = self.datetime
        if isinstance(self.datetime, np.datetime64):
            timestamp = (
                (dt - np.datetime64('1970-01-01T00:00:00')) /
                np.timedelta64(1, 's')
            )
            dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        return dt.strftime(format)

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of datetime and values
        :rtype: dict
        """
        return {
            'datetime': self._datetime_as_str(),
            'values': self.values
        }


class PointTimelineValues:

    def __init__(self, location: Point, values: List[DatasetTimelineValue]) -> None:
        self.location = location
        self.values = values


class DatasetReaderValue2:

    date_variable = 'date'
    chunk_size_in_bytes = 81920  # 80KB chunks

    def __init__(
            self, val: Union[xrDataset, List[PointTimelineValues]],
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute]) -> None:
        self._val = val
        self._is_xr_dataset = isinstance(val, xrDataset)
        self.location_input = location_input
        self.attributes = attributes
        self._post_init()

    def _post_init(self):
        """Rename source variable into attribute name."""
        if not self._is_xr_dataset:
            return
        renamed_dict = {}
        for attr in self.attributes:
            renamed_dict[attr.source] = attr.attribute.variable_name
        self._val = self._val.rename(renamed_dict)

    @property
    def xr_dataset(self) -> xrDataset:
        return self._val

    @property
    def values(self) -> List[PointTimelineValues]:
        return self._val

    def is_empty(self) -> bool:
        """Check if value is empty.

        :return: True if empty dataset or empty list
        :rtype: bool
        """
        if self._is_xr_dataset:
            return self._val is None
        return len(self.values) == 0

    def _to_dict(self) -> dict:
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        if self.location_input is None or len(self.values) == 0:
            return {}

        return {
            'geometry': json.loads(self.location_input.point.json),
            'data': [result.to_dict() for result in self.values[0].values]
        }

    def _xr_dataset_to_dict(self) -> dict:
        """Convert xArray Dataset to dictionary.

        Implementation depends on provider.
        :return: data dictionary
        :rtype: dict
        """
        return {}

    def to_json(self) -> dict:
        """Convert result to json.

        :raises TypeError: if location input is not a Point
        :return: data dictionary
        :rtype: dict
        """
        if self.location_input.type != LocationInputType.POINT:
            raise TypeError('Location input type is not point!')
        if self._is_xr_dataset:
            return self._xr_dataset_to_dict()
        return self._to_dict()

    def to_netcdf_stream(self):
        """Generate netcdf stream."""
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=True, delete_on_close=False) as tmp_file:
            self.xr_dataset.to_netcdf(tmp_file.name, format='NETCDF4', engine='netcdf4')
            with open(tmp_file.name, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream."""
        dim_order = [self.date_variable]
        reordered_cols = [attribute.attribute.variable_name for attribute in self.attributes]
        if 'lat' in self.xr_dataset.dims:
            dim_order.append('lat')
            dim_order.append('lon')
        else:
            reordered_cols.insert(0, 'lon')
            reordered_cols.insert(0, 'lat')
        df = self.xr_dataset.to_dataframe(dim_order=dim_order)
        df_reordered = df[reordered_cols]
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True, delete_on_close=False) as tmp_file:
            df_reordered.to_csv(tmp_file.name, index=True, header=True, sep=separator, mode='w', lineterminator='\n', float_format='%.4f')
            with open(tmp_file.name, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk


class DatasetReaderValue:
    """Class representing all values from reader."""

    def __init__(
            self, location: Point,
            results: List[DatasetTimelineValue]) -> None:
        """Initialize DatasetReaderValue object.

        :param location: point to the observed station/grid cell
        :type location: Point
        :param results: Data value list
        :type results: List[DatasetTimelineValue]
        """
        self.location = location
        self.results = results

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        if self.location is None:
            return {}
        return {
            'geometry': json.loads(self.location.json),
            'data': [result.to_dict() for result in self.results]
        }


class LocationDatasetReaderValue(DatasetReaderValue):
    """Class representing data values for multiple locations."""

    def __init__(
            self, results: Dict[Point, List[DatasetTimelineValue]]) -> None:
        """Initialize LocationDatasetReaderValue."""
        super().__init__(None, [])
        self.results = results

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        location_data = []
        for location, values in self.results.items():
            val = DatasetReaderValue(location, values)
            location_data.append(val.to_dict())
        return location_data


class BaseDatasetReader:
    """Base class for Dataset Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime,
            output_type=DatasetReaderOutputType.JSON) -> None:
        """Initialize BaseDatasetReader class.

        :param dataset: Dataset for reading
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        :param output_type: Output type
        :type output_type: str
        """
        self.dataset = dataset
        self.attributes = attributes
        self.location_input = location_input
        self.start_date = start_date
        self.end_date = end_date
        self.output_type = output_type

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

    def read(self):
        """Read values from dataset."""
        today = datetime.now(tz=pytz.UTC)
        if self.dataset.type.type == CastType.HISTORICAL:
            self.read_historical_data(
                self.start_date,
                self.end_date if self.end_date < today else today
            )
        elif self.end_date >= today:
            self.read_forecast_data(
                self.start_date if self.start_date >= today else today,
                self.end_date
            )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        pass

    def get_data_values2(self) -> DatasetReaderValue2:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        pass


    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        pass

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        pass
