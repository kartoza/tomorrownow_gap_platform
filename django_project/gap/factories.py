# ground_observations/factories.py
import factory
from factory.django import DjangoModelFactory
from gap.models import (
    Provider, Attribute, Country, Station, Measurement
)
from django.contrib.gis.geos import Point, MultiPolygon, Polygon


class ProviderFactory(DjangoModelFactory):
    class Meta:
        model = Provider

    name = factory.Faker('company')
    description = factory.Faker('text')


class AttributeFactory(DjangoModelFactory):
    class Meta:
        model = Attribute

    name = factory.Sequence(
        lambda n: f'attribute-{n}'
    )
    description = factory.Faker('text')


class CountryFactory(DjangoModelFactory):
    class Meta:
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
    class Meta:
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
    class Meta:
        model = Measurement

    station = factory.SubFactory(StationFactory)
    attribute = factory.SubFactory(AttributeFactory)
    date = factory.Faker('date')
    value = factory.Faker('pyfloat')
