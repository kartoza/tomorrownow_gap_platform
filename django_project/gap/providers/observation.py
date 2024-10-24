# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Observation Data Reader
"""

from datetime import datetime
import numpy as np
import pandas as pd
import xarray as xr
import tempfile
from django.db.models import Exists, OuterRef
from django.contrib.gis.geos import Polygon, Point
from django.contrib.gis.db.models.functions import Distance
from typing import List, Tuple

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement,
    DatasetTimeStep,
    DatasetObservationType
)
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetTimelineValue,
    BaseDatasetReader,
    DatasetReaderValue
)


class CSVBuffer:
    """An object that implements the write method of file-like interface."""

    def write(self, value):
        """Return the string to write."""
        yield value


class ObservationReaderValue(DatasetReaderValue):
    """Class that convert Dataset to TimelineValues."""

    date_variable = 'date'

    def __init__(
            self, val: List[DatasetTimelineValue],
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            start_date: datetime,
            end_date: datetime,
            nearest_stations) -> None:
        """Initialize ObservationReaderValue class.

        :param val: value that has been read
        :type val: List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(val, location_input, attributes)
        self.start_date = start_date
        self.end_date = end_date
        self.nearest_stations = nearest_stations

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream.

        :param suffix: file extension, defaults to '.csv'
        :type suffix: str, optional
        :param separator: separator, defaults to ','
        :type separator: str, optional
        :yield: bytes of csv file
        :rtype: bytes
        """
        dataset = self.attributes[0].dataset
        time_col_exists = dataset.time_step != DatasetTimeStep.DAILY
        alt_col_exists = (
            dataset.observation_type ==
            DatasetObservationType.UPPER_AIR_OBSERVATION
        )
        headers = ['date']

        # add time if time_step is not daily
        if time_col_exists:
            headers.append('time')

        # add lat and lon
        headers.extend(['lat', 'lon'])

        # add altitude if it's upper air observation
        if alt_col_exists:
            headers.append('altitude')

        # write headers
        for attr in self.attributes:
            headers.append(attr.attribute.variable_name)
        yield bytes(','.join(headers) + '\n', 'utf-8')

        for val in self.values:
            data = [val.get_datetime_repr('%Y-%m-%d')]

            # add time if time_step is not daily
            if time_col_exists:
                data.append(val.get_datetime_repr('%H:%M:%S'))

            # add lat and lon
            data.extend([
                str(val.location.y),
                str(val.location.x)
            ])

            # add altitude if it's upper air observation
            if alt_col_exists:
                data.append(
                    str(val.altitude) if val.altitude else ''
                )

            for attr in self.attributes:
                var_name = attr.attribute.variable_name
                if var_name in val.values:
                    data.append(str(val.values[var_name]))
                else:
                    data.append('')

            # write row
            yield bytes(','.join(data) + '\n', 'utf-8')

    def _get_date_array(self):
        """Get date range from result values."""
        dataset = self.attributes[0].dataset

        first_dt = self.values[0].get_datetime()
        last_dt = self.values[-1].get_datetime()

        if dataset.time_step == DatasetTimeStep.DAILY:
            first_dt = first_dt.date()
            last_dt = last_dt.date()

        return pd.date_range(
            first_dt, last_dt,
            freq=DatasetTimeStep.to_freq(dataset.time_step))

    def _get_date_index(
            self, date_array: pd.DatetimeIndex, datetime: datetime):
        """Get date index from date_array."""
        dataset = self.attributes[0].dataset
        dt = (
            datetime.replace(hour=0, minute=0, second=0, tzinfo=None) if
            dataset.time_step == DatasetTimeStep.DAILY else datetime
        )
        return date_array.get_loc(dt)

    def to_netcdf_stream(self):
        """Generate NetCDF."""
        # create date array
        date_array = self._get_date_array()

        # sort lat and lon array
        lat_array = set()
        lon_array = set()
        station: Station
        for station in self.nearest_stations:
            x = round(station.geometry.x, 5)
            y = round(station.geometry.y, 5)
            lon_array.add(x)
            lat_array.add(y)
        lat_array = sorted(lat_array)
        lon_array = sorted(lon_array)
        lat_array = pd.Index(lat_array, dtype='float64')
        lon_array = pd.Index(lon_array, dtype='float64')

        # define the data variables
        data_vars = {}
        empty_shape = (len(date_array), len(lat_array), len(lon_array))
        for attr in self.attributes:
            var = attr.attribute.variable_name
            data_vars[var] = (
                ['date', 'lat', 'lon'],
                np.empty(empty_shape)
            )

        # create the dataset
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'date': ('date', date_array),
                'lat': ('lat', lat_array),
                'lon': ('lon', lon_array)
            }
        )

        # assign values to the dataset
        for val in self.values:
            date_idx = self._get_date_index(date_array, val.get_datetime())
            loc = val.location
            lat_idx = lat_array.get_loc(round(loc.y, 5))
            lon_idx = lon_array.get_loc(round(loc.x, 5))

            for attr in self.attributes:
                var_name = attr.attribute.variable_name
                if var_name not in val.values:
                    continue
                ds[var_name][date_idx, lat_idx, lon_idx] = (
                    val.values[var_name]
                )

        # write to netcdf
        with (
            tempfile.NamedTemporaryFile(
                suffix=".nc", delete=True, delete_on_close=False)
        ) as tmp_file:
            ds.to_netcdf(
                tmp_file.name, format='NETCDF4', engine='netcdf4')
            with open(tmp_file.name, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk


class ObservationDatasetReader(BaseDatasetReader):
    """Class to read observation ground observation data."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime,
            altitudes: Tuple[float, float] = None
    ) -> None:
        """Initialize ObservationDatasetReader class.

        :param dataset: Dataset from observation provider
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
            dataset, attributes, location_input, start_date, end_date,
            altitudes=altitudes
        )
        self.results: List[DatasetTimelineValue] = []
        self.nearest_stations = None

    def query_by_altitude(self, qs):
        """Query by altitude."""
        altitudes = self.altitudes
        try:
            if altitudes[0] is not None and altitudes[1] is not None:
                qs = qs.filter(
                    altitude__gte=altitudes[0]
                ).filter(
                    altitude__lte=altitudes[1]
                )
        except (IndexError, TypeError):
            pass
        return qs

    def _find_nearest_station_by_point(self, point: Point = None):
        p = point
        if p is None:
            p = self.location_input.point
        # has_measurement is for removing duplicates station
        qs = Station.objects.annotate(
            distance=Distance('geometry', p),
            has_measurement=Exists(
                Measurement.objects.filter(
                    station=OuterRef('pk'),
                    dataset_attribute__dataset=self.dataset
                )
            )
        ).filter(
            provider=self.dataset.provider,
            has_measurement=True
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

    def get_nearest_stations(self):
        """Return nearest stations."""
        nearest_stations = None
        if self.location_input.type == LocationInputType.POINT:
            nearest_stations = self._find_nearest_station_by_point()
        elif self.location_input.type == LocationInputType.POLYGON:
            nearest_stations = self._find_nearest_station_by_polygon()
        elif self.location_input.type == LocationInputType.LIST_OF_POINT:
            nearest_stations = self._find_nearest_station_by_points()
        elif self.location_input.type == LocationInputType.BBOX:
            nearest_stations = self._find_nearest_station_by_bbox()
        return nearest_stations

    def get_measurements(self, start_date: datetime, end_date: datetime):
        """Return measurements data."""
        self.nearest_stations = self.get_nearest_stations()
        if self.nearest_stations is None or len(self.nearest_stations) == 0:
            return
        return Measurement.objects.select_related(
            'dataset_attribute', 'dataset_attribute__attribute',
            'station', 'station_history'
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station__in=self.nearest_stations
        ).order_by('date_time', 'station', 'dataset_attribute')

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        measurements = self.get_measurements(start_date, end_date)
        if measurements is None or measurements.count() == 0:
            return

        # final result, group by datetime
        self.results = []

        iter_dt = None
        iter_loc = None
        iter_alt = None
        # group by location and date_time
        dt_loc_val = {}
        for measurement in measurements:
            # if it has history, use history location
            station_history = measurement.station_history
            if station_history:
                measurement_loc = station_history.geometry
                measurement_alt = station_history.altitude
            else:
                measurement_loc = measurement.station.geometry
                measurement_alt = measurement.station.altitude

            if iter_dt is None:
                iter_dt = measurement.date_time
                iter_loc = measurement_loc
                iter_alt = measurement_alt
            elif (
                    iter_loc != measurement_loc or
                    iter_dt != measurement.date_time or
                    iter_alt != measurement_alt
            ):
                self.results.append(
                    DatasetTimelineValue(
                        iter_dt, dt_loc_val, iter_loc, iter_alt
                    )
                )
                iter_dt = measurement.date_time
                iter_loc = measurement_loc
                iter_alt = measurement_alt
                dt_loc_val = {}
            dt_loc_val[
                measurement.dataset_attribute.attribute.variable_name
            ] = measurement.value
        self.results.append(
            DatasetTimelineValue(
                iter_dt, dt_loc_val, iter_loc, iter_alt
            )
        )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return ObservationReaderValue(
            self.results, self.location_input, self.attributes,
            self.start_date, self.end_date, self.nearest_stations
        )
