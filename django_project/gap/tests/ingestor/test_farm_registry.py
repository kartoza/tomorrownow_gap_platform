# coding: utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCASFarmRegistryIngestor.
"""

import os
import logging
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from gap.models import (
    Farm, Crop, FarmRegistry, FarmRegistryGroup,
    IngestorSession, IngestorSessionStatus
)
from gap.ingestor.farm_registry import DCASFarmRegistryIngestor


logger = logging.getLogger(__name__)


class DCASFarmRegistryIngestorTest(TestCase):
    """Unit tests for DCASFarmRegistryIngestor."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json'
    ]

    def setUp(self):
        """Set up test case."""
        self.test_zip_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',  # Test data directory
            'farm_registry',
            'test_farm_registry.zip'  # Pre-existing ZIP file
        )

    def test_successful_ingestion(self):
        """Test successful ingestion of farmer registry data."""
        with open(self.test_zip_path, 'rb') as _file:
            test_file = SimpleUploadedFile(_file.name, _file.read())

        session = IngestorSession.objects.create(
            file=test_file,
            ingestor_type='Farm Registry',
            trigger_task=False
        )

        ingestor = DCASFarmRegistryIngestor(session)
        ingestor.run()

        # Verify session status
        session.refresh_from_db()
        print(session.status, session.notes)
        self.assertEqual(
            session.status,
            IngestorSessionStatus.SUCCESS,
            "Session status should be SUCCESS."
        )

        # Verify FarmRegistryGroup was created
        self.assertEqual(FarmRegistryGroup.objects.count(), 1)
        group = FarmRegistryGroup.objects.first()
        self.assertTrue(group.is_latest)

        # Verify Farm and FarmRegistry were created
        self.assertEqual(Farm.objects.count(), 2)
        self.assertEqual(FarmRegistry.objects.count(), 2)

        # Verify specific farm details
        farm = Farm.objects.get(unique_id='F001')
        self.assertEqual(farm.geometry.x, 36.8219)
        self.assertEqual(farm.geometry.y, -1.2921)

        # Verify Crop details
        crop = Crop.objects.get(name='Maize')
        self.assertIsNotNone(crop)
