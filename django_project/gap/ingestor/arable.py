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
    Provider, Station, ObservationType, CollectorSession, Dataset, DatasetType,
    DatasetTimeStep, DatasetStore, Country
)
from gap.models.preferences import Preferences

PROVIDER = 'Arable'
OBSERVATION_TYPE = 'Ground Observations'
DATASET_TYPE = 'Observations'
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


class ArableIngestor(BaseIngestor):
    """Ingestor for arable data."""

    api_key = None

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)

        self.provider = Provider.objects.get(
            name=PROVIDER
        )
        self.obs_type = ObservationType.objects.get(
            name=OBSERVATION_TYPE
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

    def get(self, url, params=None, page=1):
        """Request the API."""
        if params is None:
            params = {}

        if not self.api_key:
            raise ApiKeyNotFoundException()

        params['page'] = page
        response = requests.get(
            url, params=params, headers={
                'Authorization': f'Apikey {self.api_key}'
            }
        )

        if response.status_code == 200:
            return response.json()['items'] + self.get(url, params, page + 1)
        else:
            return []

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
            Station.objects.get_or_create(
                code=device['id'],
                provider=self.provider,
                defaults={
                    'name': device['name'],
                    'geometry': point,
                    'country': country,
                    'observation_type': self.obs_type,
                }
            )
