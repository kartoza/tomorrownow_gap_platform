# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm Registry models
"""

from django.db import models
from django.utils import timezone

from core.models.common import Definition
from gap.models.farm import Farm
from gap.models.crop_insight import Crop, CropStageType, CropGrowthStage
from gap.models.common import Country


class FarmRegistryGroup(Definition):
    """Model that represents group of FarmRegistry."""

    date_time = models.DateTimeField(
        default=timezone.now,
        help_text='The time when the registry is ingested.'
    )
    country = models.ForeignKey(
        Country, on_delete=models.CASCADE,
        blank=True, null=True
    )
    is_latest = models.BooleanField(default=False)

    class Meta:  # noqa: D106
        ordering = ['-date_time']


class FarmRegistry(models.Model):
    """Model that represents FarmRegistry."""

    group = models.ForeignKey(FarmRegistryGroup, on_delete=models.CASCADE)
    farm = models.ForeignKey(Farm, on_delete=models.CASCADE)
    crop = models.ForeignKey(Crop, on_delete=models.CASCADE)
    crop_stage_type = models.ForeignKey(
        CropStageType,
        on_delete=models.CASCADE
    )
    planting_date = models.DateField()
    crop_growth_stage = models.ForeignKey(
        CropGrowthStage,
        on_delete=models.CASCADE,
        blank=True,
        null=True
    )
    growth_stage_start_date = models.DateField(
        blank=True,
        null=True,
        help_text='Start date when the growth stage is updated.'
    )

    class Meta:  # noqa
        unique_together = (
            'group', 'farm', 'crop', 'crop_stage_type',
            'planting_date'
        )
