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
from django.db.models import Exists, OuterRef, F, FloatField, QuerySet
from django.db.models.functions.datetime import TruncDate, TruncTime
from django.contrib.gis.geos import Polygon, Point
from django.contrib.gis.db.models.functions import Distance, GeoFunc
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
    BaseDatasetReader,
    DatasetReaderValue
)


class ST_X(GeoFunc):
    """Custom GeoFunc to extract lon."""

    output_field = FloatField()
    function = 'ST_X'


class ST_Y(GeoFunc):
    """Custom GeoFunc to extract lat."""

    output_field = FloatField()
    function = 'ST_Y'


class CSVBuffer:
    """An object that implements the write method of file-like interface."""

    def write(self, value):
        """Return the string to write."""
        yield value


class ObservationReaderValue(DatasetReaderValue):
    """Class that convert Dataset to TimelineValues."""

    date_variable = 'date'
    csv_chunk_size = 50000

    def __init__(
            self, val: QuerySet,
            location_input: DatasetReaderInput,
            attributes: List[DatasetAttribute],
            start_date: datetime,
            end_date: datetime,
            nearest_stations,
            result_count) -> None:
        """Initialize ObservationReaderValue class.

        :param val: value that has been read
        :type val: List[DatasetTimelineValue]
        :param location_input: location input query
        :type location_input: DatasetReaderInput
        :param attributes: list of dataset attributes
        :type attributes: List[DatasetAttribute]
        """
        super().__init__(
            val, location_input, attributes,
            result_count=result_count
        )
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
        fields = {
            'date': TruncDate('date_time'),
            'loc_x': ST_X('geom'),
            'loc_y': ST_Y('geom'),
            'attr_id': F('dataset_attribute__attribute__id'),
        }
        field_index = [
            'date'
        ]

        # add time if time_step is not daily
        if time_col_exists:
            headers.append('time')
            fields.update({
                'time': TruncTime('date_time')
            })
            field_index.append('time')

        # add lat and lon
        headers.extend(['lat', 'lon'])
        field_index.extend(['loc_y', 'loc_x'])

        # add altitude if it's upper air observation
        if alt_col_exists:
            headers.append('altitude')
            fields.update({
                'loc_alt': F('alt')
            })
            field_index.append('loc_alt')

        # write headers
        sorted_attrs = sorted(self.attributes, key=lambda x: x.attribute.id)
        for attr in sorted_attrs:
            headers.append(attr.attribute.variable_name)
        yield bytes(','.join(headers) + '\n', 'utf-8')

        # annotate and select required fields only
        measurements = self._val.annotate(**fields).values(
            *(list(fields.keys()) + ['value'])
        )
        # Convert to DataFrame
        df = pd.DataFrame(list(measurements))

        # Pivot the data to make attributes columns
        df_pivot = df.pivot_table(
            index=field_index,
            columns='attr_id',
            values='value'
        ).reset_index()

        # add other attributes
        for attr in sorted_attrs:
            if attr.attribute.id not in df_pivot.columns:
                df_pivot[attr.attribute.id] = None

        # reorder columns
        df_pivot = df_pivot[
            field_index + [attr.attribute.id for attr in sorted_attrs]
        ]

        # Write the data in chunks
        for start in range(0, len(df), self.csv_chunk_size):
            chunk = df_pivot.iloc[start:start + self.csv_chunk_size]
            yield chunk.to_csv(index=False, header=False, float_format='%g')

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
        self.results: QuerySet = QuerySet.none
        self.result_count = 0
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
        if self.nearest_stations is None or self.nearest_stations.count() == 0:
            return

        return Measurement.objects.annotate(
            geom=F('station__geometry'),
            alt=F('station__altitude')
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
            dataset_attribute__in=self.attributes,
            station__in=self.nearest_stations
        ).order_by('date_time')

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        measurements = self.get_measurements(start_date, end_date)
        self.result_count = measurements.count()
        if measurements is None or self.result_count == 0:
            return

        self.results = measurements

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        return ObservationReaderValue(
            self.results, self.location_input, self.attributes,
            self.start_date, self.end_date, self.nearest_stations,
            self.result_count
        )
