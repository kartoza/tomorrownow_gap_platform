# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading dataset
"""

from typing import Union, List, Dict
import numpy as np
from datetime import datetime
import pytz
from django.contrib.gis.geos import Point

from gap.models import (
    Dataset,
    DatasetAttribute
)


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
        if isinstance(self.datetime, np.datetime64):
            return np.datetime_as_string(
                self.datetime, unit='s', timezone='UTC')
        return self.datetime.isoformat(timespec='seconds')

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of datetime and values
        :rtype: dict
        """
        return {
            'datetime': self._datetime_as_str(),
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


class LocationDatasetReaderValue(DatasetReaderValue):
    """Class representing data values for multiple locations."""

    def __init__(
            self, metadata: dict,
            results: Dict[Point, List[DatasetTimelineValue]]) -> None:
        """Initialize LocationDatasetReaderValue."""
        super().__init__(metadata, [])
        self.results = results

    def to_dict(self):
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        location_data = []
        for location, values in self.results.items():
            location_data.append({
                'lat': location.y,
                'lon': location.x,
                'data': [result.to_dict() for result in values]
            })
        return {
            'metadata': self.metadata,
            'data': location_data
        }


class LocationInputType:
    """Class for data input type."""

    POINT = 'point'
    BBOX = 'bbox'


class DatasetReaderInput:
    """Class to store the dataset reader input.

    Input type: Point, bbox
    TODO: list of points, polygon
    """

    def __init__(self, points: List[Point], is_bbox: bool = False):
        """Initialize DatasetReaderInput class."""
        self.points = points
        self.is_bbox = is_bbox

    def get_input_type(self) -> str:
        """Get input type.

        :return: LocationInputType
        :rtype: str
        """
        if len(self.points) == 2 and self.is_bbox:
            return LocationInputType.BBOX
        return LocationInputType.POINT


class BaseDatasetReader:
    """Base class for Dataset Reader."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput,
            start_date: datetime, end_date: datetime) -> None:
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
        """
        self.dataset = dataset
        self.attributes = attributes
        self.location_input = location_input
        self.start_date = start_date
        self.end_date = end_date

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
        if self.start_date < today:
            self.read_historical_data(
                self.start_date,
                self.end_date if self.end_date < today else today
            )
            if self.end_date > today:
                self.read_forecast_data(
                    today, self.end_date
                )
        else:
            self.read_forecast_data(
                self.start_date, self.end_date
            )

    def get_data_values(self) -> DatasetReaderValue:
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
