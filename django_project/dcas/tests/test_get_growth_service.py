# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Test Service for Growth Stage
"""

from unittest.mock import patch
from django.test import TestCase
from dcas.models import GDDMatrix
from dcas.service import GrowthStageService


class GrowthStageServiceTest(TestCase):
    """Test Growth Stage Service."""

    fixtures = [
        '6.unit.json',
        '7.attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '1.dcas_config.json',
        '14.crop.json',
        '15.gdd_config.json',
        '16.gdd_matrix.json'
    ]

    @patch("dcas.service.cache")
    def test_get_growth_stage_from_cache(self, mock_cache):
        """Test retrieving growth stage from mocked cache."""
        # Mock cache.get to return a pre-cached value
        mock_cache.get.return_value = [
            {
                "gdd_threshold": 100,
                "crop_growth_stage__id": 1,
                "crop_growth_stage__name": "Germination"
            },
            {
                "gdd_threshold": 150,
                "crop_growth_stage__id": 2,
                "crop_growth_stage__name": "Establishment"
            },
        ]

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 150)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 2)
        self.assertEqual(stage["label"], "Establishment")

        # Verify cache.get was called
        mock_cache.get.assert_called_once_with("gdd_matrix:2:1")

    @patch("dcas.service.cache")
    def test_get_growth_stage_database_fetch(self, mock_cache):
        """Test retrieving growth stage when cache is empty."""
        # Mock cache.get to return None
        mock_cache.get.return_value = None

        # Mock cache.set to simulate caching
        mock_cache.set = patch("dcas.service.cache.set").start()

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 150)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 2)
        self.assertEqual(stage["label"], "Establishment")

        # Verify cache.set was called to populate the cache
        mock_cache.set.assert_called_once()

    @patch("dcas.service.cache")
    def test_get_growth_stage_early_upper(self, mock_cache):
        """Test retrieving growth stage for high GDD values."""
        # Mock cache.get to return appropriate stage thresholds
        mock_cache.get.side_effect = lambda key: [
            {
                "gdd_threshold": 800,
                "crop_growth_stage__id": 7,
                "crop_growth_stage__name": "Grain Filling"
            },
            {
                "gdd_threshold": 900,
                "crop_growth_stage__id": 8,
                "crop_growth_stage__name": "Physiological Maturity"
            },
        ] if "8" in key else None

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 1000)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 8)
        self.assertEqual(stage["label"], "Physiological Maturity")

        # Verify cache.get was called
        mock_cache.get.assert_called_with("gdd_matrix:2:1")

    @patch("dcas.service.cache")
    def test_get_growth_stage_below_threshold(self, mock_cache):
        """Test retrieving growth stage below all thresholds."""
        # Mock cache.get to return stages for crop 9, stage type 2
        mock_cache.get.return_value = [
            {
                "gdd_threshold": 100,
                "crop_growth_stage__id": 10,
                "crop_growth_stage__name": "Maturity"
            },
            {
                "gdd_threshold": 200,
                "crop_growth_stage__id": 11,
                "crop_growth_stage__name": "Seedling"
            },
        ]

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(9, 2, 200)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 11)
        self.assertEqual(stage["label"], "Seedling")

        # Verify cache.get was called
        mock_cache.get.assert_called_once_with("gdd_matrix:9:2")

    @patch("dcas.service.cache")
    def test_get_growth_stage_no_matrix(self, mock_cache):
        """Test when no GDDMatrix exists for the crop and stage type."""
        # Mock cache.get to return None
        mock_cache.get.return_value = None

        # Ensure no matrix exists in the database
        GDDMatrix.objects.filter(crop_id=99, crop_stage_type_id=99).delete()

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(99, 99, 150)
        self.assertIsNone(stage)

        # Verify cache.get was called
        mock_cache.get.assert_called_once_with("gdd_matrix:99:99")
