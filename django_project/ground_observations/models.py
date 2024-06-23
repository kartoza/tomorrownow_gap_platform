# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models

from core.models.general import Definition
import uuid


class Provider(Definition):
    """Model representing a data provider.

    Override Definition model that contains name and description .
    """
    name = models.CharField(max_length=255)
    provider = models.UUIDField(
        unique=True, default=uuid.uuid4, editable=False
    )
    description = models.TextField()

    def __str__(self):
        return self.name


class Attribute(Definition):
    """Model representing an attribute of a measurement.

    Override Definition model that contains name and description .
    """

    name = models.CharField(max_length=255)
    attribute = models.UUIDField(
        unique=True, default=uuid.uuid4, editable=False
    )
    description = models.TextField()

    def __str__(self):
        return self.name


class Country(Definition):
    """Model representing a country.

    Override Definition model that contains name and description .

    Attributes:
        name (str): Name of the country.
        iso_a3 (str): ISO A3 country code, unique.
        geometry (Polygon):
            MultiPolygonField geometry representing the country boundaries.
    """

    name = models.CharField(max_length=255)
    iso_a3 = models.TextField(
        unique=True
    )
    geometry = models.MultiPolygonField(
        srid=4326
    )
    description = models.TextField()

    class Meta:  # noqa
        verbose_name_plural = "countries"

    def __str__(self):
        return self.name


class Station(Definition):
    """Model representing a ground observation station.

    Override Definition model that contains name and description .

    Attributes:
        name (str): Name of the station.
        country (ForeignKey):
            Foreign key referencing the Country model based on country_ISO_A3.
        geometry (Point):
            Point geometry representing the location of the station.
        provider (ForeignKey):
            Foreign key referencing the Provider model based on provider_id.
    """

    name = models.CharField(max_length=255)
    station = models.UUIDField(
        unique=True, default=uuid.uuid4, editable=False
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
    description = models.TextField()

    def __str__(self):
        return self.name


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
