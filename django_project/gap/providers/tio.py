# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tomorrow.io Data Reader
"""

import os
import json
import logging
from typing import List
from datetime import datetime, timedelta
import pytz
import requests
from django.contrib.gis.geos import Point

from gap.models import (
    Provider,
    CastType,
    DatasetType,
    Dataset,
    DatasetAttribute,
    DatasetTimeStep,
    DatasetStore
)
from gap.utils.reader import (
    LocationInputType,
    DatasetVariable,
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseDatasetReader
)

logger = logging.getLogger(__name__)
PROVIDER_NAME = 'Tomorrow.io'
TIO_VARIABLES = {
    'rainAccumulationSum': DatasetVariable(
        'Rain Accumulation',
        'The accumulated amount of liquid rain',
        'mm', 'total_rainfall'
    ),
    'evapotranspirationSum': DatasetVariable(
        'Evapotranspiration',
        'The combined processes by which water moves from '
        'the earth\'s surface into the atmosphere',
        'mm', 'total_evapotranspiration_flux'
    ),
    'temperatureMax': DatasetVariable(
        'Temperature Max',
        '',
        '°C', 'max_total_temperature'
    ),
    'temperatureMin': DatasetVariable(
        'Temperature Min',
        '',
        '°C', 'min_total_temperature'
    )
}


class TomorrowIODatasetReader(BaseDatasetReader):
    """Class to read data from Tomorrow.io API."""

    LONG_TERM_NORMALS_TYPE = 'Long Term Normals (20 years)'
    BASE_URL = 'https://api.tomorrow.io/v4'

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize Dataset Reader."""
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)
        self.errors = None
        self.warnings = None
        self.results = []

    @classmethod
    def init_provider(cls):
        """Init Tomorrow.io provider and variables."""
        provider, _ = Provider.objects.get_or_create(name='Tomorrow.io')
        dt_historical, _ = DatasetType.objects.get_or_create(
            name='Historical Reanalysis',
            defaults={
                'type': CastType.HISTORICAL
            }
        )
        ds_historical, _ = Dataset.objects.get_or_create(
            name=f'{provider.name} {dt_historical.name}',
            provider=provider,
            type=dt_historical,
            store_type=DatasetStore.EXT_API,
            defaults={
                'time_step': DatasetTimeStep.DAILY,
                'is_internal_use': True
            }
        )
        dt_shorttermforecast, _ = DatasetType.objects.get_or_create(
            name='Short-term Forecast',
            defaults={
                'type': CastType.FORECAST
            }
        )
        ds_forecast, _ = Dataset.objects.get_or_create(
            name=f'{provider.name} {dt_shorttermforecast.name}',
            provider=provider,
            type=dt_shorttermforecast,
            store_type=DatasetStore.EXT_API,
            defaults={
                'time_step': DatasetTimeStep.DAILY,
                'is_internal_use': True
            }
        )
        dt_ltn, _ = DatasetType.objects.get_or_create(
            name=cls.LONG_TERM_NORMALS_TYPE,
            defaults={
                'type': CastType.HISTORICAL
            }
        )
        ds_ltn, _ = Dataset.objects.get_or_create(
            name=f'{provider.name} {dt_ltn.name}',
            provider=provider,
            type=dt_ltn,
            store_type=DatasetStore.EXT_API,
            defaults={
                'time_step': DatasetTimeStep.DAILY,
                'is_internal_use': True
            }
        )

        for key, val in TIO_VARIABLES.items():
            attr = val.get_gap_attribute()
            # add to dataset attribute
            DatasetAttribute.objects.get_or_create(
                dataset=ds_historical,
                attribute=attr,
                source=key,
                source_unit=attr.unit
            )
            DatasetAttribute.objects.get_or_create(
                dataset=ds_forecast,
                attribute=attr,
                source=key,
                source_unit=attr.unit
            )
            DatasetAttribute.objects.get_or_create(
                dataset=ds_ltn,
                attribute=attr,
                source=key,
                source_unit=attr.unit
            )

    def _is_ltn_request(self):
        """Check if the request is for Long Term Normal (LTN) request."""
        return (
            self.dataset.type.type == CastType.HISTORICAL and
            self.dataset.type.name == self.LONG_TERM_NORMALS_TYPE
        )

    def _get_api_key(self):
        """Retrieve API Key for Tomorrow.io."""
        return os.environ.get('TOMORROW_IO_API_KEY', '')

    def _get_headers(self):
        """Get request headers."""
        return {
            'Accept-Encoding': 'gzip',
            'accept': 'application/json',
            'content-type': 'application/json'
        }

    def _get_payload(
            self, start_date: datetime, end_date: datetime,
            is_ltn: bool = False):
        """Get request payload.

        This method will normalize the start_date if
        start_date and date is less than 24H.

        :param start_date: _description_
        :type start_date: datetime
        :param end_date: _description_
        :type end_date: datetime
        :param is_ltn: _description_, defaults to False
        :type is_ltn: bool, optional
        :return: _description_
        :rtype: _type_
        """
        start_dt = start_date
        if (end_date - start_dt).total_seconds() < 24 * 3600:
            start_dt = start_dt - timedelta(days=1)
        payload = {
            'location': (
                f'{self.location_input.point.y}, '
                f'{self.location_input.point.x}'
            ),
            'fields': [attr.source for attr in self.attributes],
            'timesteps': ['1d'],
            'units': 'metric',
        }
        if is_ltn:
            payload.update({
                'startDate': start_dt.strftime('%m-%d'),
                'endDate': end_date.strftime('%m-%d')
            })
        else:
            payload.update({
                'startTime': (
                    start_dt.isoformat(
                        timespec='seconds').replace("+00:00", "Z")
                ),
                'endTime': (
                    end_date.isoformat(
                        timespec='seconds').replace("+00:00", "Z")
                ),
            })
        return payload

    def read(self):
        """Read values from Tomorrow.io API."""
        self.results = []
        self.errors = None
        self.warnings = None
        today = datetime.now(tz=pytz.UTC)
        if self.location_input.type != LocationInputType.POINT:
            return
        # handles:
        # - start_date=end_date
        # - d-7 should be using timelines API
        # - historical/timelines may return the same day,
        #   choosing to use historical
        if self._is_ltn_request():
            self._read_ltn_data(
                self.start_date,
                self.end_date
            )
        elif self.dataset.type.type == CastType.HISTORICAL:
            max_date = today - timedelta(days=7)
            if self.start_date < max_date:
                self.read_historical_data(
                    self.start_date,
                    self.end_date if self.end_date < max_date else
                    max_date
                )
            if self.end_date >= max_date:
                # read from forecast data
                start_dt = self.start_date
                if max_date > start_dt:
                    start_dt = max_date + timedelta(days=1)
                self.read_forecast_data(
                    start_dt,
                    self.end_date
                )
        else:
            self.read_forecast_data(
                self.start_date if self.start_date >= today else today,
                self.end_date
            )

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch data values from dataset.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        if self.location_input.type != LocationInputType.POINT:
            return DatasetReaderValue(Point(x=0, y=0, srid=4326), [])
        if not self.is_success():
            logger.error(f'Tomorrow.io API errors: {len(self.errors)}')
            logger.error(json.dumps(self.errors))
            return DatasetReaderValue(self.location_input.point, [])
        if self.warnings:
            logger.warn(f'Tomorrow.io API warnings: {len(self.warnings)}')
            logger.warn(json.dumps(self.warnings))
        return DatasetReaderValue(self.location_input.point, self.results)

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        url = f'{self.BASE_URL}/historical?apikey={self._get_api_key()}'
        payload = self._get_payload(start_date, end_date)
        response = requests.post(
            url, json=payload, headers=self._get_headers())
        if response.status_code != 200:
            self._get_error_from_response(response)
            return
        self.results.extend(self._parse_result(response.json()))

    def read_forecast_data(self, start_date: datetime, end_date: datetime):
        """Read forecast data from dataset.

        :param start_date: start date for reading forecast data
        :type start_date: datetime
        :param end_date:  end date for reading forecast data
        :type end_date: datetime
        """
        url = f'{self.BASE_URL}/timelines?apikey={self._get_api_key()}'
        payload = self._get_payload(start_date, end_date)
        response = requests.post(
            url, json=payload, headers=self._get_headers())
        if response.status_code != 200:
            self._get_error_from_response(response)
            return
        self.results.extend(self._parse_result(response.json()))

    def _read_ltn_data(self, start_date: datetime, end_date: datetime):
        """Read Long Term Normals (LTN) data.

        :param start_date: start date for reading data
        :type start_date: datetime
        :param end_date: end date for reading data
        :type end_date: datetime
        """
        url = (
            f'{self.BASE_URL}/historical/normals?apikey={self._get_api_key()}'
        )
        payload = self._get_payload(start_date, end_date, is_ltn=True)
        response = requests.post(
            url, json=payload, headers=self._get_headers())
        if response.status_code != 200:
            self._get_error_from_response(response)
            return
        self.results = self._parse_result(response.json())

    def _get_error_from_response(self, response):
        """Get error detail from Tomorrow.io API response.

        :param response: API response
        :type response: response object
        """
        error = "Unknown error!"
        try:
            result = response.json()
            error = f"{result.get('type', '')} {result.get('message', '')}"
        except Exception:
            pass
        if self.errors is None:
            self.errors = [error]
        else:
            self.errors.append(error)

    def _get_result_datetime(self, interval: dict) -> datetime:
        """Parse datetime from API response.

        :param interval: interval dictionary
        :type interval: dict
        :return: datetime
        :rtype: datetime
        """
        dt_str = interval.get('startTime')
        if self._is_ltn_request():
            dt_str = interval.get('startDate')
            dt_str = f'{self.start_date.year}-{dt_str}'
            return datetime.strptime(
                dt_str, '%Y-%m-%d').replace(tzinfo=pytz.utc)
        return datetime.fromisoformat(dt_str)

    def _parse_result(self, result: dict) -> List[DatasetTimelineValue]:
        """Parse successful response from Tomorrow.io API.

        This method also checks for any warnings in the response.
        :param result: response data
        :type result: dict
        :return: data values
        :rtype: List[DatasetTimelineValue]
        """
        value_list = []
        data = result.get('data', {})
        timelines = data.get('timelines', [])
        intervals = (
            timelines[0].get('intervals', []) if len(timelines) > 0 else []
        )
        for interval in intervals:
            start_dt = self._get_result_datetime(interval)
            if start_dt < self.start_date or start_dt > self.end_date:
                continue
            values = interval.get('values')
            value_data = {}
            for attribute in self.attributes:
                value_data[attribute.attribute.variable_name] = (
                    values.get(attribute.source, None)
                )
            value_list.append(DatasetTimelineValue(
                start_dt,
                value_data
            ))
        warnings = data.get('warnings', None)
        if warnings:
            if self.warnings is None:
                self.warnings = warnings
            else:
                self.warnings.extend(warnings)
        return value_list

    def is_success(self) -> bool:
        """Check whether the API requests are successful.

        :return: True if there is no errors
        :rtype: bool
        """
        return self.errors is None
