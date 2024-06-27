# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from django.contrib.gis.geos import Point, MultiPolygon, Polygon

from core.factories import BaseMetaFactory, BaseFactory
from gap.models import (
    Provider,
    Attribute,
    Country,
    Station,
    Measurement,
    ObservationType
)


class ProviderFactory(
    BaseFactory[Provider], metaclass=BaseMetaFactory[Provider]
):
    """Factory class for Provider model."""

    class Meta:  # noqa
        model = Provider

    name = factory.Faker('company')
    description = factory.Faker('text')


class AttributeFactory(
    BaseFactory[Attribute], metaclass=BaseMetaFactory[Attribute]
):
    """Factory class for Attribute model."""

    class Meta:  # noqa
        model = Attribute

    name = factory.Sequence(
        lambda n: f'attribute-{n}'
    )
    description = factory.Faker('text')


class ObservationTypeFactory(
    BaseFactory[ObservationType], metaclass=BaseMetaFactory[ObservationType]
):
    """Factory class for ObservationType model."""

    class Meta:  # noqa
        model = ObservationType

    name = factory.Sequence(
        lambda n: f'observation-type-{n}'
    )
    description = factory.Faker('text')


class CountryFactory(
    BaseFactory[Country], metaclass=BaseMetaFactory[Country]
):
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


class StationFactory(
    BaseFactory[Station], metaclass=BaseMetaFactory[Station]
):
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
    observation_type = factory.SubFactory(ObservationTypeFactory)


class MeasurementFactory(
    BaseFactory[Measurement], metaclass=BaseMetaFactory[Measurement]
):
    """Factory class for Measurement model."""

    class Meta:  # noqa
        model = Measurement

    station = factory.SubFactory(StationFactory)
    attribute = factory.SubFactory(AttributeFactory)
    date = factory.Faker('date')
    value = factory.Faker('pyfloat')
