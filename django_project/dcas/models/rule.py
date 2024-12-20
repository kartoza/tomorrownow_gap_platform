# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Rule
"""

from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

from gap.models import Crop, CropGrowthStage, CropStageType, Attribute
from dcas.models.config import DCASConfig


class DCASRule(models.Model):
    """Model that represents rule."""

    config = models.ForeignKey(
        DCASConfig, on_delete=models.CASCADE
    )

    crop = models.ForeignKey(
        Crop, on_delete=models.CASCADE
    )

    crop_stage_type = models.ForeignKey(
        CropStageType, on_delete=models.CASCADE
    )

    crop_growth_stage = models.ForeignKey(
        CropGrowthStage, on_delete=models.CASCADE
    )

    parameter = models.ForeignKey(
        Attribute, on_delete=models.CASCADE
    )

    min_range = models.FloatField()
    max_range = models.FloatField()
    code = models.CharField(max_length=50)

    class Meta:  # noqa
        db_table = 'dcas_rule'
        verbose_name = _('Rule')
