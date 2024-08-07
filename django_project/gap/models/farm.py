# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm models
"""

from django.contrib.gis.db import models

from core.models.common import Definition
from gap.models.common import Crop, Village


class FarmCategory(Definition):
    """Model representing category of a farm."""

    class Meta:  # noqa
        verbose_name_plural = 'Farm categories'


class FarmRSVPStatus(Definition):
    """Model representing status of a farm."""

    class Meta:  # noqa
        verbose_name = 'Farm RSVP status'
        verbose_name_plural = 'Farm RSVP statuses'


class Farm(models.Model):
    """Model representing a farm.

    Attributes:
        unique_id (str): Unique id of the farm.
        geometry (Point): Location of the farm.
    """

    unique_id = models.CharField(
        unique=True,
        max_length=255
    )
    geometry = models.PointField(
        srid=4326
    )
    rsvp_status = models.ForeignKey(
        FarmRSVPStatus, on_delete=models.CASCADE
    )
    category = models.ForeignKey(
        FarmCategory, on_delete=models.CASCADE
    )
    crop = models.ForeignKey(
        Crop, on_delete=models.CASCADE
    )
    village = models.ForeignKey(
        Village, on_delete=models.SET_NULL, null=True, blank=True
    )
