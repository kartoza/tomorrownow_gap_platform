# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Observation Data Reader
"""

import json
from datetime import datetime
import pandas as pd
import tempfile
from django.db.models import Exists, OuterRef, F, FloatField, QuerySet
from django.db.models.functions.datetime import TruncDate, TruncTime
from django.contrib.gis.geos import Polygon, Point
from django.contrib.gis.db.models.functions import Distance, GeoFunc
from typing import List, Tuple, Union

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement
)
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    BaseDatasetReader,
    DatasetReaderValue
)
from gap.utils.dask import execute_dask_compute


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
        self.attributes = sorted(
            self.attributes, key=lambda x: x.attribute.id
        )

    def _get_data_frame(self, use_separate_time_col=True) -> pd.DataFrame:
        """Create a dataframe from query result.

        :return: Data frame
        :rtype: pd.DataFrame
        """
        fields = {
            'date': (
                TruncDate('date_time') if use_separate_time_col else
                F('date_time')
            ),
            'loc_x': ST_X('geom'),
            'loc_y': ST_Y('geom'),
            'attr_id': F('dataset_attribute__attribute__id'),
        }
        field_index = [
            'date'
        ]

        # add time if time_step is not daily
        if self.has_time_column and use_separate_time_col:
            fields.update({
                'time': TruncTime('date_time')
            })
            field_index.append('time')

        # add lat and lon
        field_index.extend(['loc_y', 'loc_x'])

        # add altitude if it's upper air observation
        if self.has_altitude_column:
            fields.update({
                'loc_alt': F('alt')
            })
            field_index.append('loc_alt')

        # annotate and select required fields only
        measurements = self._val.annotate(**fields).values(
            *(list(fields.keys()) + ['value'])
        )
        # Convert to DataFrame
        df = pd.DataFrame(list(measurements))

        # Pivot the data to make attributes columns
        df = df.pivot_table(
            index=field_index,
            columns='attr_id',
            values='value'
        ).reset_index()

        # add other attributes
        for attr in self.attributes:
            if attr.attribute.id not in df.columns:
                df[attr.attribute.id] = None

        # reorder columns
        df = df[
            field_index + [attr.attribute.id for attr in self.attributes]
        ]

        return df

    def _get_headers(self, use_separate_time_col=True):
        """Get list of headers that allign with dataframce columns."""
        headers = ['date']

        # add time if time_step is not daily
        if self.has_time_column and use_separate_time_col:
            headers.append('time')

        # add lat and lon
        headers.extend(['lat', 'lon'])

        # add altitude if it's upper air observation
        if self.has_altitude_column:
            headers.append('altitude')

        field_indices = [header for header in headers]

        # add headers
        for attr in self.attributes:
            headers.append(attr.attribute.variable_name)

        return headers, field_indices

    def to_csv_stream(self, suffix='.csv', separator=','):
        """Generate csv bytes stream.

        :param suffix: file extension, defaults to '.csv'
        :type suffix: str, optional
        :param separator: separator, defaults to ','
        :type separator: str, optional
        :yield: bytes of csv file
        :rtype: bytes
        """
        headers, _ = self._get_headers()

        # write headers
        yield bytes(','.join(headers) + '\n', 'utf-8')

        # get dataframe
        df_pivot = self._get_data_frame()

        # Write the data in chunks
        for start in range(0, len(df_pivot), self.csv_chunk_size):
            chunk = df_pivot.iloc[start:start + self.csv_chunk_size]
            yield chunk.to_csv(index=False, header=False, float_format='%g')

    def to_netcdf_stream(self):
        """Generate NetCDF."""
        time_col_exists = self.has_time_column

        # if time column exists, in netcdf we should use datetime
        # instead of separating the date and time columns
        headers, field_indices = self._get_headers(
            use_separate_time_col=not time_col_exists
        )

        # get dataframe
        df_pivot = self._get_data_frame(
            use_separate_time_col=not time_col_exists
        )

        # rename columns
        if time_col_exists:
            headers[0] = 'time'
            field_indices[0] = 'time'
        df_pivot.columns = headers

        # convert date/datetime objects
        date_coord = 'date' if not time_col_exists else 'time'
        df_pivot[date_coord] = pd.to_datetime(df_pivot[date_coord])

        # Convert to xarray Dataset
        ds = df_pivot.set_index(field_indices).to_xarray()

        # write to netcdf
        with (
            tempfile.NamedTemporaryFile(
                suffix=".nc", delete=True, delete_on_close=False)
        ) as tmp_file:
            x = ds.to_netcdf(
                tmp_file.name, format='NETCDF4', engine='netcdf4',
                compute=False
            )
            execute_dask_compute(x, is_api=True)
            with open(tmp_file.name, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size_in_bytes)
                    if not chunk:
                        break
                    yield chunk

    def _to_dict(self) -> dict:
        """Convert into dict.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        if (
            self.location_input is None or self._val is None or
            self.count() == 0
        ):
            return {}

        has_altitude = self.has_altitude_column
        output = {
            'geometry': json.loads(self.location_input.geometry.json),
            'data': []
        }

        # get dataframe
        df_pivot = self._get_data_frame(
            use_separate_time_col=False
        )
        for _, row in df_pivot.iterrows():
            values = {}
            for attr in self.attributes:
                values[attr.attribute.variable_name] = row[attr.attribute.id]
            output['data'].append({
                'datetime': row['date'].isoformat(timespec='seconds'),
                'values': values
            })
            if has_altitude:
                output['altitude'] = row['loc_alt']

        return output


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

    def _get_count(self, values: Union[List, QuerySet]) -> int:
        """Get count of a list of queryset.

        :param values: List or QuerySet object
        :type values: Union[List, QuerySet]
        :return: count
        :rtype: int
        """
        if isinstance(values, list):
            return len(values)
        return values.count()

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
        if (
            self.nearest_stations is None or
            self._get_count(self.nearest_stations) == 0
        ):
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
