# coding: utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCASFarmRegistryIngestor.
"""

import os
import logging
import unittest
from unittest.mock import patch
from datetime import date
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from gap.models import (
    Farm, FarmRegistry, FarmRegistryGroup,
    IngestorSession, IngestorSessionStatus
)
from gap.ingestor.farm_registry import (
    DCASFarmRegistryIngestor, Keys
)


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
        with open(self.test_zip_path, 'rb') as _file:
            self.test_file = SimpleUploadedFile(_file.name, _file.read())

        self.session = IngestorSession.objects.create(
            file=self.test_file,
            ingestor_type='Farm Registry',
            trigger_task=False
        )

        self.ingestor = DCASFarmRegistryIngestor(self.session)

    def test_successful_ingestion(self):
        """Test successful ingestion of farmer registry data."""
        self.ingestor.run()

        # Verify session status
        self.session.refresh_from_db()
        print(self.session.status, self.session.notes)
        self.assertEqual(
            self.session.status,
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

    def test_bulk_insert_with_empty_farm_list(self):
        """Test `_bulk_insert_farms_and_registries()`."""
        # Ensure farm_list is empty
        self.ingestor.farm_list = []

        # Patch bulk_create to ensure it does NOT get called
        with patch("gap.models.Farm.objects.bulk_create") as mock_bulk_create:
            self.ingestor._bulk_insert_farms_and_registries()

            # Assert bulk_create was NEVER called
            mock_bulk_create.assert_not_called()

    @patch(
        "gap.ingestor.farm_registry.DCASFarmRegistryIngestor._run",
        side_effect=Exception("Fatal error")
    )
    def test_run_failure_sets_failed_status(self, mock_run):
        """Ensure session status is marked as FAILED."""
        self.ingestor.run()

        # Refresh session and check status
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, IngestorSessionStatus.FAILED)
        self.assertIn("Fatal error", self.session.notes)

    def test_stage_lookup_population(self):
        """Ensure `_process_row` correctly updates `stage_lookup`."""
        # Create a CropStageType instance
        row = {
            'CropName': 'Maize_Mid',
            'FarmerId': 'F100',
            'FinalLatitude': '36.8219',
            'FinalLongitude': '-1.2921',
            'PlantingDate': '2024-01-01'
        }

        self.ingestor._process_row(row)

        # Ensure that the crop stage type is added to stage_lookup
        crop_stage_key = "mid"
        self.assertIn(crop_stage_key, self.ingestor.stage_lookup)


class TestKeysStaticMethods(unittest.TestCase):
    """Test static methods in Keys class."""

    def test_get_crop_key(self):
        """Test get_crop_key."""
        self.assertEqual(
            Keys.get_crop_key({'CropName': 'Maize'}), 'CropName')
        self.assertEqual(
            Keys.get_crop_key({'crop': 'Cassava'}), 'crop')
        with self.assertRaises(KeyError):
            Keys.get_crop_key({'wrong_key': 'Soybean'})

    def test_get_planting_date_key(self):
        """Test get_planting_date_key."""
        self.assertEqual(
            Keys.get_planting_date_key(
                {'PlantingDate': '2024-01-01'}), 'PlantingDate')
        self.assertEqual(
            Keys.get_planting_date_key(
                {'plantingDate': '2024-01-01'}), 'plantingDate')
        with self.assertRaises(KeyError):
            Keys.get_planting_date_key({'date': '2024-01-01'})

    def test_get_farm_id_key(self):
        """Test get_farm_id_key."""
        self.assertEqual(
            Keys.get_farm_id_key({'FarmerId': '123'}), 'FarmerId')
        self.assertEqual(
            Keys.get_farm_id_key({'farmer_id': '456'}), 'farmer_id')
        with self.assertRaises(KeyError):
            Keys.get_farm_id_key({'id': '789'})


class TestDCASFarmRegistryIngestorStaticMethods(unittest.TestCase):
    """Test static methods in DCASFarmRegistryIngestor."""

    def setUp(self):
        """Set up a test instance of DCASFarmRegistryIngestor."""
        self.ingestor = DCASFarmRegistryIngestor(None)

    def test_parse_valid_planting_dates(self):
        """Test parsing valid planting dates."""
        self.assertEqual(
            self.ingestor._parse_planting_date(
                "01/15/2024"), date(2024, 1, 15))
        self.assertEqual(
            self.ingestor._parse_planting_date(
                "2024-01-15"), date(2024, 1, 15))
        self.assertEqual(
            self.ingestor._parse_planting_date(
                "15-01-2024"), date(2024, 1, 15))

    def test_parse_invalid_planting_date(self):
        """Test parsing an invalid planting date."""
        self.assertIsNone(
            self.ingestor._parse_planting_date("2024/Jan/15"))
        self.assertIsNone(
            self.ingestor._parse_planting_date("not a date"))
