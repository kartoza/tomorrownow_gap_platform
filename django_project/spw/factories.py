# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from django.contrib.gis.geos import Point
from django.db.models.signals import post_save, post_delete

from core.factories import BaseMetaFactory, BaseFactory, UserF
from spw.models import (
    RModel,
    RModelOutput,
    RModelExecutionLog,
    RModelOutputType
)


@factory.django.mute_signals(post_save, post_delete)
class RModelFactory(
    BaseFactory[RModel], metaclass=BaseMetaFactory[RModel]
):
    """Factory class for RModel model."""

    class Meta:  # noqa
        model = RModel

    name = factory.Faker('company')
    version = 1.0
    code = 'd <- 100 + 2'
    notes = factory.Faker('text')
    created_on = factory.Faker('date_time')
    updated_on = factory.Faker('date_time')
    created_by = factory.SubFactory(UserF)
    updated_by = factory.SubFactory(UserF)


class RModelOutputFactory(
    BaseFactory[RModelOutput], metaclass=BaseMetaFactory[RModelOutput]
):
    """Factory class for RModelOutput."""

    class Meta:  # noqa
        model = RModelOutput

    model = factory.SubFactory(RModelFactory)
    type = RModelOutputType.GO_NO_GO_STATUS
    variable_name = RModelOutputType.GO_NO_GO_STATUS


class RModelExecutionLogFactory(
    BaseFactory[RModelExecutionLog],
    metaclass=BaseMetaFactory[RModelExecutionLog]
):
    """Factory class for RModelExecutionLog."""

    class Meta:  # noqa
        model = RModelExecutionLog

    model = factory.SubFactory(RModelFactory)
    location_input = factory.LazyFunction(lambda: Point(0, 0))
    start_date_time = factory.Faker('date_time')
    end_date_time = factory.Faker('date_time')
