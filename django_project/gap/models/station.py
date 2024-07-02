# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Point  # noqa

from core.models.common import Definition
from gap.models.common import Country


class Provider(Definition):
    """Model representing a data provider."""

    pass


class ObservationType(Definition):
    """Model representing an observation type."""

    pass


class Station(Definition):
    """Model representing a ground observation station.

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
    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    observation_type = models.ForeignKey(
        ObservationType, on_delete=models.CASCADE
    )

    class Meta:  # noqa
        unique_together = ('code', 'provider')
