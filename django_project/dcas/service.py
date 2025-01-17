# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Service for Growth Stage
"""

from django.core.cache import cache
from dcas.models import GDDMatrix


class GrowthStageService:
    """A service to get the growth stage."""

    CACHE_KEY_TEMPLATE = "gdd_matrix:{crop_id}:{crop_stage_type_id}"

    @staticmethod
    def get_growth_stage(crop_id, crop_stage_type_id, total_gdd):
        """
        Get the growth stage: crop ID, stage type, and total GDD.

        :param crop_id: ID of the crop
        :type crop_id: int
        :param crop_stage_type_id: ID of the crop stage type
        :type crop_stage_type_id: int
        :param total_gdd: Total accumulated GDD
        :type total_gdd: float
        :return: Dictionary containing growth id, label, and gdd_threshold
        :rtype: dict or None
        """
        cache_key = GrowthStageService.CACHE_KEY_TEMPLATE.format(
            crop_id=crop_id, crop_stage_type_id=crop_stage_type_id
        )
        growth_stage_matrix = cache.get(cache_key)

        if growth_stage_matrix is None:
            # Load the matrix from the database if not in cache
            growth_stage_matrix = list(
                GDDMatrix.objects.filter(
                    crop_id=crop_id,
                    crop_stage_type_id=crop_stage_type_id
                )
                .select_related("crop_growth_stage")
                .order_by("gdd_threshold")
                .values(
                    "gdd_threshold",
                    "crop_growth_stage__id",
                    "crop_growth_stage__name"
                )
            )
            cache.set(cache_key, growth_stage_matrix, timeout=None)

        # Find the appropriate growth stage based on total GDD
        for stage in growth_stage_matrix:
            if total_gdd <= stage["gdd_threshold"]:
                return {
                    "id": stage["crop_growth_stage__id"],
                    "label": stage["crop_growth_stage__name"],
                    "gdd_threshold": stage["gdd_threshold"]
                }

        # Return the last stage if total GDD exceeds all thresholds
        if growth_stage_matrix:
            last_stage = growth_stage_matrix[-1]
            return {
                "id": last_stage["crop_growth_stage__id"],
                "label": last_stage["crop_growth_stage__name"],
                "gdd_threshold": last_stage["gdd_threshold"]
            }

        # No growth stage found
        return None
