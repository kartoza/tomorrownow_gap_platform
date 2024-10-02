# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point  # noqa

from core.models.common import Definition
from gap.models.common import Country, Provider


class StationType(Definition):
    """Model representing an observation type."""

    pass


class Station(Definition):
    """Model representing an observation station.

    Override Definition model that contains name and description .

    Attributes:
        name (str): Name of the station.
        code (str): Code of the station, unique.
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
    altitude = models.FloatField(
        null=True, blank=True, help_text='Altitude in meters'
    )
    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    station_type = models.ForeignKey(
        StationType, on_delete=models.CASCADE
    )
    metadata = models.JSONField(
        null=True, blank=True
    )

    class Meta:  # noqa
        unique_together = ('code', 'provider')


class StationHistory(models.Model):
    """Model representing station histories.

    Override Definition model that contains name and description .
    """

    station = models.ForeignKey(
        Station, on_delete=models.CASCADE
    )
    geometry = models.PointField(
        srid=4326
    )
    altitude = models.FloatField(
        null=True, blank=True, help_text='Altitude in meters'
    )
    date_time = models.DateTimeField()

    class Meta:  # noqa
        unique_together = ('station', 'date_time')
