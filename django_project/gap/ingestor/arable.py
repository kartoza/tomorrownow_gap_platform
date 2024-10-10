# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo ingestor.
"""

import os

import requests
from django.contrib.gis.geos import Point

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import ApiKeyNotFoundException
from gap.models import (
    Provider, Station, StationType, IngestorSession, Dataset, DatasetType,
    DatasetTimeStep, DatasetStore, Country, Measurement
)
from gap.models.preferences import Preferences

PROVIDER = 'Arable'
STATION_TYPE = 'Ground Observations'
DATASET_TYPE = 'Arable Ground Observation'
DATASET_NAME = 'Arable Ground Observational'
API_KEY_ENV_NAME = 'ARABLE_API_KEY'


class ArableAPI:
    """Arable API."""

    def __init__(self):
        """Initialize Arable API."""
        base_url = Preferences.load().arable_api_url
        if not base_url:
            raise Exception('Base URL for arable is not set.')

        self.DEVICES = f'{base_url}/devices'
        self.DATA = f'{base_url}/data/daily'


class ArableIngestor(BaseIngestor):
    """Ingestor for arable data."""

    api_key = None

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)

        self.provider = Provider.objects.get(
            name=PROVIDER
        )
        self.station_type = StationType.objects.get(
            name=STATION_TYPE
        )
        self.dataset_type = DatasetType.objects.get(
            name=DATASET_TYPE
        )
        self.dataset, _ = Dataset.objects.get_or_create(
            name=DATASET_NAME,
            provider=self.provider,
            type=self.dataset_type,
            time_step=DatasetTimeStep.DAILY,
            store_type=DatasetStore.TABLE
        )

        self.attributes = {}
        for dataset_attr in self.dataset.datasetattribute_set.all():
            self.attributes[dataset_attr.source] = dataset_attr.id

    def get(self, url, params=None, page=1, is_pagination=True):
        """Request the API."""
        if params is None:
            params = {}

        if not self.api_key:
            raise ApiKeyNotFoundException()

        if is_pagination:
            params['page'] = page

        response = requests.get(
            url, params=params, headers={
                'Authorization': f'Apikey {self.api_key}'
            }
        )

        if response.status_code == 200:
            if is_pagination:
                return response.json()['items'] + self.get(
                    url, params, page + 1, is_pagination=is_pagination
                )
            else:
                return response.json()
        else:
            return []

    @staticmethod
    def last_iso_date_time(station: Station) -> str | None:
        """Last date measurements of station."""
        first_measurement = Measurement.objects.filter(
            station=station
        ).order_by('-date_time').first()
        if first_measurement:
            return first_measurement.date_time.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        return None

    def get_data(self, station: Station):
        """Get data of the given station."""
        keys = list(self.attributes.keys()) + ['time']
        keys.sort()
        params = {
            'device': station.name,
            'select': ','.join(keys)
        }
        last_time = ArableIngestor.last_iso_date_time(station)
        if last_time:
            params['start_time'] = last_time
        data = self.get(
            ArableAPI().DATA, params=params, is_pagination=False
        )
        for row in data:
            for source, attr_id in self.attributes.items():
                try:
                    value = row[source]
                    if value is not None:
                        Measurement.objects.get_or_create(
                            station=station,
                            dataset_attribute_id=attr_id,
                            date_time=row['time'],
                            defaults={
                                'value': value
                            }
                        )
                except KeyError:
                    pass

    def run(self):
        """Run the ingestor."""
        self.api_key = os.environ.get(API_KEY_ENV_NAME, None)

        # Get stations or devices
        devices = self.get(ArableAPI().DEVICES)
        for device in devices:
            # Skip device that does not have location
            try:
                point = Point(
                    x=device['current_location']['gps'][0],
                    y=device['current_location']['gps'][1],
                    srid=4326
                )
            except (KeyError, IndexError):
                continue

            # Get country
            try:
                country = Country.get_countries_by_point(
                    point
                )[0]
            except IndexError:
                country = None

            # Create station
            station, _ = Station.objects.get_or_create(
                code=device['id'],
                provider=self.provider,
                defaults={
                    'name': device['name'],
                    'geometry': point,
                    'country': country,
                    'station_type': self.station_type,
                }
            )

            # Get station data
            self.get_data(station)
