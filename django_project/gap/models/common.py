# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point

from core.models.common import Definition


class Country(Definition):
    """Model representing a country.

    Attributes:
        name (str): Name of the country.
        iso_a3 (str): ISO A3 country code, unique.
        geometry (Polygon):
            MultiPolygonField geometry representing the country boundaries.
    """

    iso_a3 = models.CharField(
        unique=True,
        max_length=255
    )
    geometry = models.MultiPolygonField(
        srid=4326,
        blank=True,
        null=True
    )

    class Meta:  # noqa
        verbose_name_plural = 'countries'
        ordering = ['name']

    @staticmethod
    def get_countries_by_point(point: Point):
        """Get country by point."""
        return Country.objects.filter(
            geometry__contains=point
        )


class Provider(Definition):
    """Model representing a data provider."""

    pass


class Unit(Definition):
    """Model representing an unit of a measurement."""

    pass


class Crop(Definition):
    """Model representing crop."""

    pass
