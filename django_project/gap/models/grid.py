# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Grid Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point

from core.models.common import Definition
from gap.models.common import Country


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
    country = models.ForeignKey(
        Country, on_delete=models.SET_NULL,
        null=True, blank=True
    )

    @staticmethod
    def get_grids_by_point(point: Point):
        """Get grids by point."""
        return Grid.objects.filter(
            geometry__contains=point
        )
