# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Farm Models.
"""
import factory
from django.contrib.gis.geos import Point

from core.factories import BaseMetaFactory, BaseFactory
from gap.factories.main import CropFactory
from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm
)


class FarmCategoryFactory(
    BaseFactory[FarmCategory], metaclass=BaseMetaFactory[FarmCategory]
):
    """Factory class for FarmCategory model."""

    class Meta:  # noqa
        model = FarmCategory

    name = factory.Sequence(
        lambda n: f'farm-category-{n}'
    )
    description = factory.Faker('text')


class FarmRSVPStatusFactory(
    BaseFactory[FarmRSVPStatus], metaclass=BaseMetaFactory[FarmRSVPStatus]
):
    """Factory class for FarmRSVPStatus model."""

    class Meta:  # noqa
        model = FarmRSVPStatus

    name = factory.Sequence(
        lambda n: f'farm-rsvp-status-{n}'
    )
    description = factory.Faker('text')


class FarmFactory(
    BaseFactory[Farm], metaclass=BaseMetaFactory[Farm]
):
    """Factory class for Farm model."""

    class Meta:  # noqa
        model = Farm

    unique_id = factory.Sequence(
        lambda n: f'id-{n}'
    )
    geometry = factory.LazyFunction(lambda: Point(0, 0))
    rsvp_status = factory.SubFactory(FarmRSVPStatusFactory)
    category = factory.SubFactory(FarmCategoryFactory)
    crop = factory.SubFactory(CropFactory)
