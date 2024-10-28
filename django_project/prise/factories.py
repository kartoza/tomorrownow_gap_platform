# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Message
"""
import factory
from factory.django import DjangoModelFactory

from gap.factories.crop_insight import PestFactory
from gap.factories.farm import FarmFactory
from prise.models import (
    PriseMessage,
    PrisePest,
    PriseData,
    PriseDataByPest,
    PriseMessageSchedule
)
from prise.variables import PriseMessageGroup


class PriseMessageFactory(DjangoModelFactory):
    """Factory class for PriseMessage model."""

    class Meta:  # noqa
        model = PriseMessage

    pest = factory.SubFactory(PestFactory)


class PrisePestFactory(DjangoModelFactory):
    """Factory class for PrisePest model."""

    class Meta:  # noqa
        model = PrisePest

    pest = factory.SubFactory(PestFactory)
    variable_name = factory.Sequence(
        lambda n: f'pest-{n}'
    )


class PriseDataFactory(DjangoModelFactory):
    """Factory class for PriseData model."""

    class Meta:  # noqa
        model = PriseData

    farm = factory.SubFactory(FarmFactory)
    ingested_at = factory.Faker('date')
    generated_at = factory.Faker('date')


class PriseDataByPestFactory(DjangoModelFactory):
    """Factory class for PriseDataByPest model."""

    class Meta:  # noqa
        model = PriseDataByPest

    data = factory.SubFactory(PriseDataFactory)
    pest = factory.SubFactory(PestFactory)
    value = factory.Faker('pyfloat')


class PriseMessageScheduleFactory(DjangoModelFactory):
    """Factory class for PriseMessageSchedule model."""

    class Meta:  # noqa
        model = PriseMessageSchedule

    group = PriseMessageGroup.START_SEASON
    week_of_month = 1
    day_of_week = 1
    active = True
    priority = 1
