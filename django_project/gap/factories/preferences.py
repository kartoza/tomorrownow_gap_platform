# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Preferences
"""
import factory
from django.contrib.gis.geos import Polygon
from factory.django import DjangoModelFactory

from gap.models import Preferences


class PreferencesFactory(DjangoModelFactory):
    """Factory class for Provider model."""

    class Meta:  # noqa
        model = Preferences

    area_of_interest = factory.LazyFunction(
        lambda: Polygon([
            [0.0, 0.0],
            [50.0, 0.0],
            [100.0, 50.0],
            [100.0, 100.0],
            [50.0, 100.0],
            [0.0, 50.0],
            [0.0, 0.0]
        ])
    )
