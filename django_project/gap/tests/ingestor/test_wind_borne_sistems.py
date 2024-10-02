# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Windborne Systems Ingestor.
"""
import os

import responses
from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import GEOSGeometry
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.ingestor.exceptions import EnvIsNotSetException
from gap.ingestor.wind_borne_systems import (
    WindBorneSystemsAPI,
    USERNAME_ENV_NAME, PASSWORD_ENV_NAME
)
from gap.models import (
    Country, Station, IngestorSession, IngestorSessionStatus, IngestorType
)
from gap.tests.mock_response import BaseTestWithPatchResponses, PatchReqeust


class WindBorneSystemsAPIIngestorTest(BaseTestWithPatchResponses, TestCase):
    """WindBorneSystems ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.WIND_BORNE_SYSTEMS_API
    responses_folder = absolute_path(
        'gap', 'tests', 'ingestor', 'data', 'windbornesystems'
    )

    @property
    def mock_requests(self):
        """Mock requests."""
        base_url = WindBorneSystemsAPI.base_url
        return [
            PatchReqeust(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'since_1.json'
                )
            ),
            PatchReqeust(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'since=1727308800'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'since_2.json'
                )
            ),
            PatchReqeust(
                (
                    f'{base_url}/observations.json?'
                    f'include_ids=True&include_mission_name=True&'
                    f'since=1727395200'
                ),
                file_response=os.path.join(
                    self.responses_folder, 'since_3.json'
                )
            )
        ]

    def setUp(self):
        """Init test case."""
        # Init kenya Country
        shp_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'Kenya.geojson'
        )
        data_source = DataSource(shp_path)
        layer = data_source[0]
        for feature in layer:
            geometry = GEOSGeometry(feature.geom.wkt, srid=4326)
            Country.objects.create(
                name=feature['name'],
                iso_a3=feature['iso_a3'],
                geometry=geometry
            )

    def test_no_username_and_password(self):
        """Test no API Key."""
        # No username
        os.environ[USERNAME_ENV_NAME] = ''
        os.environ[PASSWORD_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(
            session.notes,
            EnvIsNotSetException(USERNAME_ENV_NAME).message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

        # No password
        os.environ[USERNAME_ENV_NAME] = 'Username'
        os.environ[PASSWORD_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(
            session.notes,
            EnvIsNotSetException(PASSWORD_ENV_NAME).message
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    @responses.activate
    def test_run(self):
        """Test run."""
        self.init_mock_requests()
        os.environ[USERNAME_ENV_NAME] = 'Username'
        os.environ[PASSWORD_ENV_NAME] = 'password'

        # First import
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        first_station = Station.objects.first()
        self.assertEqual(
            first_station.stationhistory_set.count(), 2
        )

        # Next import
        session.run()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        #
        first_station = Station.objects.get(code='mission-1')
        self.assertEqual(
            first_station.stationhistory_set.count(), 3
        )
        self.assertEqual(
            list(
                first_station.measurement_set.filter(
                    dataset_attribute__source='pressure'
                ).values_list('value', flat=True)
            ),
            [10, 20, 30]
        )
        self.assertEqual(first_station.altitude, 30)
        self.assertEqual(
            list(
                first_station.stationhistory_set.values_list(
                    'altitude', flat=True
                )
            ),
            [10, 20, 30]
        )
