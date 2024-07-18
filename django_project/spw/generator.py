# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: SPW Generator
"""

import logging
import pytz
from typing import List
from datetime import datetime, timedelta
from django.contrib.gis.geos import Point

from gap.models import Dataset, DatasetAttribute, CastType
from gap.utils.reader import DatasetReaderInput
from gap.providers import TomorrowIODatasetReader, TIO_PROVIDER
from spw.utils.plumber import (
    execute_spw_model,
    write_plumber_data,
    remove_plumber_data
)


logger = logging.getLogger(__name__)
ATTRIBUTES = [
    'total_evapotranspiration_flux',
    'total_rainfall'
]
COLUMNS = [
    'month_day',
    'date',
    'evapotranspirationSum',
    'rainAccumulationSum',
    'LTNPET',
    'LTNPrecip'
]
VAR_MAPPING = {
    'total_evapotranspiration_flux': 'evapotranspirationSum',
    'total_rainfall': 'rainAccumulationSum'
}
LTN_MAPPING = {
    'total_evapotranspiration_flux': 'LTNPET',
    'total_rainfall': 'LTNPrecip'
}


class SPWOutput:
    """Class to store the output from SPW model."""

    def __init__(
            self, point: Point, go_no_go: str, ltn_percentage: float,
            cur_percentage: float) -> None:
        """Initialize the SPWOutput class."""
        self.point = point
        self.go_no_go = go_no_go
        self.ltn_percentage = ltn_percentage
        self.cur_percentage = cur_percentage


def calculate_from_point(point: Point) -> SPWOutput:
    """Calculate SPW from given point.

    :param point: Location to be queried
    :type point: Point
    :return: Output with GoNoGo classification
    :rtype: SPWOutput
    """
    TomorrowIODatasetReader.init_provider()
    location_input = DatasetReaderInput.from_point(point)
    attrs = list(DatasetAttribute.objects.filter(
        attribute__variable_name__in=ATTRIBUTES,
        dataset__provider__name=TIO_PROVIDER
    ))
    today = datetime.now(tz=pytz.UTC)
    start_dt = today - timedelta(days=37)
    end_dt = today + timedelta(days=14)
    print(f'Today: {today} - start_dt: {start_dt} - end_dt: {end_dt}')
    historical_dict = _fetch_timelines_data(
        location_input, attrs, start_dt, end_dt
    )
    final_dict = _fetch_ltn_data(
        location_input, attrs, start_dt, end_dt, historical_dict)
    rows = []
    for month_day, val in final_dict.items():
        row = [month_day]
        for c in COLUMNS:
            if c == 'month_day':
                continue
            row.append(val.get(c, 0))
        rows.append(row)
    data_file_path = write_plumber_data(COLUMNS, rows)
    success, data = execute_spw_model(
        data_file_path, point.y, point.x, 'gap_place')
    remove_plumber_data(data_file_path)
    output = None
    if success:
        output = SPWOutput(
            point, data['goNoGo'], data['nearDaysLTNPercent'],
            data['nearDaysCurPercent'])
    return output


def _fetch_timelines_data(
        location_input: DatasetReaderInput, attrs: List[DatasetAttribute],
        start_dt: datetime, end_dt: datetime) -> dict:
    """Fetch historical and forecast data for given location.

    :param location_input: Location for the query
    :type location_input: DatasetReaderInput
    :param attrs: List of attributes
    :type attrs: List[DatasetAttribute]
    :param start_dt: Start date time
    :type start_dt: datetime
    :param end_dt: End date time
    :type end_dt: datetime
    :return: Dictionary of month_day and results
    :rtype: dict
    """
    dataset = Dataset.objects.filter(
        provider__name=TIO_PROVIDER,
        type__type=CastType.HISTORICAL
    ).exclude(
        type__name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
    ).first()
    reader = TomorrowIODatasetReader(
        dataset, attrs, location_input, start_dt, end_dt)
    reader.read()
    values = reader.get_data_values()
    results = {}
    for val in values.results:
        month_day = val.get_datetime_repr('%m-%d')
        val_dict = val.to_dict()['values']
        data = {
            'date': val.get_datetime_repr('%Y-%m-%d')
        }
        for k, v in VAR_MAPPING.items():
            data[v] = val_dict.get(k, 0)
        results[month_day] = data
    return results


def _fetch_ltn_data(
        location_input: DatasetReaderInput, attrs: List[DatasetAttribute],
        start_dt: datetime, end_dt: datetime,
        historical_dict: dict) -> dict:
    """Fetch Long Term Normals data for given location.

    The resulting data will be merged into historical_dict.

    :param location_input: Location for the query
    :type location_input: DatasetReaderInput
    :param attrs: List of attributes
    :type attrs: List[DatasetAttribute]
    :param start_dt: Start date time
    :type start_dt: datetime
    :param end_dt: End date time
    :type end_dt: datetime
    :param historical_dict: Dictionary from historical data
    :type historical_dict: dict
    :return: Merged dictinoary with LTN data
    :rtype: dict
    """
    dataset = Dataset.objects.filter(
        provider__name=TIO_PROVIDER,
        type__type=CastType.HISTORICAL,
        type__name=TomorrowIODatasetReader.LONG_TERM_NORMALS_TYPE
    ).first()
    reader = TomorrowIODatasetReader(
        dataset, attrs, location_input, start_dt, end_dt)
    reader.read()
    values = reader.get_data_values()
    for val in values.results:
        month_day = val.get_datetime_repr('%m-%d')
        if month_day in historical_dict:
            data = historical_dict[month_day]
            for k, v in LTN_MAPPING.items():
                data[v] = val.values.get(k, '')
    return historical_dict
