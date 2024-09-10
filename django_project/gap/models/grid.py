# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Grid Models
"""

from django.contrib.gis.db import models

from core.models.common import Definition


class Grid(Definition):
    """Model representing a grid system."""

    unique_id = models.CharField(
        unique=True,
        max_length=255
    )
    geometry = models.PolygonField(
        srid=4326
    )
    elevation = models.FloatField()
