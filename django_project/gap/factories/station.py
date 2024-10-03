# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Station
"""
import factory
from django.contrib.gis.geos import Point
from factory.django import DjangoModelFactory

from gap.factories.main import CountryFactory, ProviderFactory
from gap.models import Station, StationType, StationHistory


class StationTypeFactory(DjangoModelFactory):
    """Factory class for StationType model."""

    class Meta:  # noqa
        model = StationType

    name = factory.Sequence(
        lambda n: f'observation-type-{n}'
    )
    description = factory.Faker('text')


class StationFactory(DjangoModelFactory):
    """Factory class for Station model."""

    class Meta:  # noqa
        model = Station

    name = factory.Sequence(
        lambda n: f'station-{n}'
    )
    code = factory.Sequence(
        lambda n: f'code-{n}'
    )
    country = factory.SubFactory(CountryFactory)
    geometry = factory.LazyFunction(lambda: Point(0, 0))
    provider = factory.SubFactory(ProviderFactory)
    description = factory.Faker('text')
    station_type = factory.SubFactory(StationTypeFactory)


class StationHistoryFactory(DjangoModelFactory):
    """Factory class for StationHistory model."""

    class Meta:  # noqa
        model = StationHistory

    station = factory.SubFactory(StationFactory)
    geometry = factory.LazyFunction(lambda: Point(0, 0))
    altitude = factory.Faker('pyfloat')
    date_time = factory.Faker('date_time')
