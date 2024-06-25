# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from factory.django import DjangoModelFactory
from gap.models import (
    Provider, Attribute, Country, Station, Measurement
)
from django.contrib.gis.geos import Point, MultiPolygon, Polygon


class ProviderFactory(DjangoModelFactory):
    """Factory class for Provider model."""

    class Meta:  # noqa
        model = Provider

    name = factory.Faker('company')
    description = factory.Faker('text')


class AttributeFactory(DjangoModelFactory):
    """Factory class for Attribute model."""

    class Meta:  # noqa
        model = Attribute

    name = factory.Sequence(
        lambda n: f'attribute-{n}'
    )
    description = factory.Faker('text')


class CountryFactory(DjangoModelFactory):
    """Factory class for Country model."""

    class Meta:  # noqa
        model = Country

    name = factory.Faker('country')
    iso_a3 = factory.Faker('country_code')
    geometry = factory.LazyAttribute(
        lambda _: MultiPolygon(
            Polygon(((0, 0), (1, 0), (1, 1), (0, 1), (0, 0)))
        )
    )
    description = factory.Faker('text')


class StationFactory(DjangoModelFactory):
    """Factory class for Station model."""

    class Meta:  # noqa
        model = Station

    name = factory.Sequence(
        lambda n: f'station-{n}'
    )
    country = factory.SubFactory(CountryFactory)
    geometry = factory.LazyFunction(lambda: Point(0, 0))
    provider = factory.SubFactory(ProviderFactory)
    description = factory.Faker('text')
    type = 'Ground Observation'


class MeasurementFactory(DjangoModelFactory):
    """Factory class for Measurement model."""

    class Meta:  # noqa
        model = Measurement

    station = factory.SubFactory(StationFactory)
    attribute = factory.SubFactory(AttributeFactory)
    date = factory.Faker('date')
    value = factory.Faker('pyfloat')
