from django.test import TestCase
from django.core.cache import cache
from dcas.models import GDDMatrix
from dcas.service import GrowthStageService


class GrowthStageServiceTest(TestCase):
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

    def setUp(self):
        """Clear the cache before each test."""
        cache.clear()

    def test_get_growth_stage_from_cache(self):
        """Test retrieving growth stage from cache."""
        # Manually populate cache to test cache retrieval
        cache_key = GrowthStageService.CACHE_KEY_TEMPLATE.format(
            crop_id=2, crop_stage_type_id=1
        )
        growth_stage_matrix = list(
            GDDMatrix.objects.filter(crop_id=2, crop_stage_type_id=1)
            .select_related("crop_growth_stage")
            .order_by("gdd_threshold")
            .values(
                "gdd_threshold",
                "crop_growth_stage__id",
                "crop_growth_stage__name"
            )
        )
        cache.set(cache_key, growth_stage_matrix)

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 150)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 2)
        self.assertEqual(stage["label"], "Establishment")

    def test_get_growth_stage_database_fetch(self):
        """Test retrieving growth stage when cache is empty."""
        # Ensure cache is empty
        cache_key = GrowthStageService.CACHE_KEY_TEMPLATE.format(
            crop_id=2, crop_stage_type_id=1
        )
        self.assertIsNone(cache.get(cache_key))

        # Fetch growth stage
        stage = GrowthStageService.get_growth_stage(2, 1, 150)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 2)
        self.assertEqual(stage["label"], "Establishment")

        # Verify cache is populated
        cached_matrix = cache.get(cache_key)
        self.assertIsNotNone(cached_matrix)

    def test_get_growth_stage_early_upper(self):
        """Test retrieving growth stage."""

        # Maize,  Early
        stage = GrowthStageService.get_growth_stage(2, 1, 1000)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 8)
        self.assertEqual(stage["label"], "Physiological Maturity")

        # Finger Millet, Early
        stage = GrowthStageService.get_growth_stage(8, 1, 500)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 7)
        self.assertEqual(stage["label"], "Grain Filling")

    def test_get_growth_stage_below_threshold(self):
        """Test retrieving growth stage."""

        # Soybeans, Late
        stage = GrowthStageService.get_growth_stage(7, 3, 700)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 4)
        self.assertEqual(stage["label"], "Flowering")

        # Cowpea, Mid
        stage = GrowthStageService.get_growth_stage(9, 2, 200)
        self.assertIsNotNone(stage)
        self.assertEqual(stage["id"], 11)
        self.assertEqual(stage["label"], "Seedling")

    def test_get_growth_stage_no_matrix(self):
        """Test when no GDDMatrix exists for the crop and stage type."""

        stage = GrowthStageService.get_growth_stage(99, 99, 150)
        self.assertIsNone(stage)
