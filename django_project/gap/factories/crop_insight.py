# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory

from core.factories import BaseMetaFactory, BaseFactory
from gap.models import (
    Crop
)


class CropFactory(
    BaseFactory[Crop], metaclass=BaseMetaFactory[Crop]
):
    """Factory class for Crop model."""

    class Meta:  # noqa
        model = Crop

    name = factory.Sequence(
        lambda n: f'crop-{n}'
    )
    description = factory.Faker('text')
