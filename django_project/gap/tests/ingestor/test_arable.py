# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Arable Ingestor.
"""
import os

import responses
from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import GEOSGeometry
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.ingestor.arable import API_KEY_ENV_NAME, ArableAPI
from gap.ingestor.exceptions import ApiKeyNotFoundException
from gap.models.ingestor import (
    IngestorSession, IngestorSessionStatus, IngestorType
)
from gap.models.station import Country, Station
from gap.tests.mock_response import BaseTestWithPatchResponses, PatchReqeust


class ArableIngestorTest(BaseTestWithPatchResponses, TestCase):
    """Arable ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.observation_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]
    ingestor_type = IngestorType.ARABLE
    responses_folder = absolute_path(
        'gap', 'tests', 'ingestor', 'data', 'arable'
    )

    @property
    def mock_requests(self):
        """Mock requests."""
        arable_api = ArableAPI()
        return [
            # Devices API
            PatchReqeust(
                arable_api.DEVICES + '?page=1',
                file_response=os.path.join(
                    self.responses_folder, 'devices.json'
                )
            ),
            # Devices API
            PatchReqeust(
                arable_api.DEVICES + '?page=2',
                response={},
                status_code=404
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

    def test_no_api_key(self):
        """Test no API Key."""
        os.environ[API_KEY_ENV_NAME] = ''
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(session.notes, ApiKeyNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    @responses.activate
    def test_run(self):
        """Test run."""
        self.init_mock_requests()
        os.environ[API_KEY_ENV_NAME] = 'API_KEY_ENV_NAME'
        session = IngestorSession.objects.create(
            ingestor_type=self.ingestor_type
        )
        session.refresh_from_db()
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        self.assertTrue(
            'A00' in list(Station.objects.all().values_list('code', flat=True))
        )
