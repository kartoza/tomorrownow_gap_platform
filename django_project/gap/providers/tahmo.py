# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo Data Reader
"""

from typing import List
from datetime import datetime
from django.contrib.gis.geos import Polygon, Point
from django.contrib.gis.db.models.functions import Distance

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement
)
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseDatasetReader,
    LocationDatasetReaderValue
)


class TahmoDatasetReader(BaseDatasetReader):
    """Class to read Tahmo ground observation data."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize TahmoDatasetReader class.

        :param dataset: Dataset from Tahmo provider
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
        self.results = {}

    def _find_nearest_station_by_point(self, point: Point = None):
        p = point
        if p is None:
            p = self.location_input.point
        qs = Station.objects.annotate(
            distance=Distance('geometry', p)
        ).filter(
            provider=self.dataset.provider
        ).order_by('distance').first()
        if qs is None:
            return None
        return [qs]

    def _find_nearest_station_by_bbox(self):
        points = self.location_input.points
        polygon = Polygon.from_bbox(
            (points[0].x, points[0].y, points[1].x, points[1].y))
        qs = Station.objects.filter(
            geometry__within=polygon
        ).order_by('id')
        if not qs.exists():
            return None
        return qs

    def _find_nearest_station_by_polygon(self):
        qs = Station.objects.filter(
            geometry__within=self.location_input.polygon
        ).order_by('id')
        if not qs.exists():
            return None
        return qs

    def _find_nearest_station_by_points(self):
        points = self.location_input.points
        results = {}
        for point in points:
            rs = self._find_nearest_station_by_point(point)
            if rs is None:
                continue
            if rs[0].id in results:
                continue
            results[rs[0].id] = rs[0]
        return results.values()

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        nearest_stations = None
        if self.location_input.type == LocationInputType.POINT:
            nearest_stations = self._find_nearest_station_by_point()
        elif self.location_input.type == LocationInputType.POLYGON:
            nearest_stations = self._find_nearest_station_by_polygon()
        elif self.location_input.type == LocationInputType.LIST_OF_POINT:
            nearest_stations = self._find_nearest_station_by_points()
        elif self.location_input.type == LocationInputType.BBOX:
            nearest_stations = self._find_nearest_station_by_bbox()
        if nearest_stations is None:
            return
        measurements = Measurement.objects.select_related(
            'dataset_attribute', 'dataset_attribute__attribute',
            'station'
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station__in=nearest_stations
        ).order_by('station', 'date_time', 'dataset_attribute')
        # final result, group by point
        self.results = {}
        curr_point = None
        curr_dt = None
        station_results = []
        # group by date_time
        measurement_dict = {}
        for measurement in measurements:
            if curr_point is None:
                curr_point = measurement.station.geometry
                curr_dt = measurement.date_time
            elif curr_point != measurement.station.geometry:
                station_results.append(
                    DatasetTimelineValue(curr_dt, measurement_dict))
                curr_dt = measurement.date_time
                measurement_dict = {}
                self.results[curr_point] = station_results
                curr_point = measurement.station.geometry
                station_results = []
            else:
                if curr_dt != measurement.date_time:
                    station_results.append(
                        DatasetTimelineValue(curr_dt, measurement_dict))
                    curr_dt = measurement.date_time
                    measurement_dict = {}
            measurement_dict[
                measurement.dataset_attribute.attribute.variable_name
            ] = measurement.value
        station_results.append(
            DatasetTimelineValue(curr_dt, measurement_dict))
        self.results[curr_point] = station_results

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch results.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        if len(self.results.keys()) == 1:
            key = list(self.results.keys())[0]
            return DatasetReaderValue(key, self.results[key])
        return LocationDatasetReaderValue(self.results)
