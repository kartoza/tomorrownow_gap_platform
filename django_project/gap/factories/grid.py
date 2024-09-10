# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Farm Models.
"""
import factory
from django.contrib.gis.geos import Polygon
from factory.django import DjangoModelFactory

from gap.models import Grid


class GridFactory(DjangoModelFactory):
    """Factory class for Grid model."""

    class Meta:  # noqa
        model = Grid

    unique_id = factory.Sequence(
        lambda n: f'id-{n}'
    )
    geometry = factory.LazyFunction(
        lambda: Polygon(
            ((0, 0), (0, 10), (10, 10), (10, 0), (0, 0)))
    )
    name = factory.Sequence(
        lambda n: f'name-{n}'
    )
    elevation = factory.Faker('pyfloat')
