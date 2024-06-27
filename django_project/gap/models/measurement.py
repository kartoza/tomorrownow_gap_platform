# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models

from core.models.common import Definition
from gap.models.station import Station


class Attribute(Definition):
    """Model representing an attribute of a measurement."""

    pass


class Measurement(models.Model):
    """
    Model representing a measurement taken at a station.

    Attributes:
        station (ForeignKey):
            Foreign key referencing the Station model based on station_id.
        attribute (ForeignKey):
            Foreign key referencing the Attribute model based on attribute_id.
        unit (str): Unit of measurement.
        date (date): Date of the measurement.
        value (float): Value of the measurement.
    """

    station = models.ForeignKey(
        Station, on_delete=models.CASCADE
    )
    attribute = models.ForeignKey(
        Attribute, on_delete=models.CASCADE
    )
    unit = models.CharField(
        max_length=512, null=True, blank=True
    )
    date = models.DateField()
    value = models.FloatField()

    def __str__(self):
        return f'{self.date} - {self.value}'
