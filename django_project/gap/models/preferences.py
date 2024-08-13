# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Preferences

"""

from django.contrib.gis.db import models

from core.models.singleton import SingletonModel


class Preferences(SingletonModel):
    """Preference settings specifically for gap."""

    area_of_interest = models.PolygonField(
        null=True, blank=True, srid=4326
    )

    class Meta:  # noqa: D106
        verbose_name_plural = "preferences"

    def __str__(self):
        return 'Preferences'
