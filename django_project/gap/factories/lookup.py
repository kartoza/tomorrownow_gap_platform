# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Lookup
"""
import factory

from core.factories import BaseMetaFactory, BaseFactory
from gap.models import (
    RainfallClassification
)


class RainfallClassificationFactory(
    BaseFactory[RainfallClassification],
    metaclass=BaseMetaFactory[RainfallClassification]
):
    """Factory class for Provider model."""

    class Meta:  # noqa
        model = RainfallClassification

    name = factory.Faker('text')
    min_value = factory.Faker('pyfloat')
    max_value = factory.Faker('pyfloat')
