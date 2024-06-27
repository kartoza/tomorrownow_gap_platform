# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point

from core.models.common import Definition


class Provider(Definition):
    """Model representing a data provider."""

    pass


class Attribute(Definition):
    """Model representing an attribute of a measurement."""

    pass


class ObservationType(Definition):
    """Model representing an observation type."""

    pass


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


class Station(Definition):
    """Model representing a ground observation station.

    Override Definition model that contains name and description .

    Attributes:
        name (str): Name of the station.
        code (str): Code of the station.
        country (ForeignKey):
            Foreign key referencing the Country model based on country_ISO_A3.
        geometry (Point):
            Point geometry representing the location of the station.
        provider (ForeignKey):
            Foreign key referencing the Provider model based on provider_id.
    """

    code = models.CharField(
        max_length=512
    )
    country = models.ForeignKey(
        Country, on_delete=models.CASCADE
    )
    geometry = models.PointField(
        srid=4326
    )
    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    observation_type = models.ForeignKey(
        ObservationType, on_delete=models.CASCADE
    )


class Measurement(models.Model):
    """
    Model representing a measurement taken at a station.

    Attributes:
        station (ForeignKey):
            Foreign key referencing the Station model based on station_id.
        attribute (ForeignKey):
            Foreign key referencing the Attribute model based on attribute_id.
        date (date): Date of the measurement.
        value (float): Value of the measurement.
    """

    station = models.ForeignKey(
        Station, on_delete=models.CASCADE
    )
    attribute = models.ForeignKey(
        Attribute, on_delete=models.CASCADE
    )
    date = models.DateField()
    value = models.FloatField()

    def __str__(self):
        return f'{self.date} - {self.value}'
