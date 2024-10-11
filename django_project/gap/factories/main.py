# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from django.contrib.gis.geos import MultiPolygon, Polygon
from factory.django import DjangoModelFactory

from gap.models import (
    CastType,
    DatasetType,
    Dataset,
    Provider,
    Unit,
    Attribute,
    DatasetAttribute,
    Country,
    DatasetTimeStep,
    DatasetStore,
    DataSourceFile,
    Village,
    DataSourceFileCache
)


class ProviderFactory(DjangoModelFactory):
    """Factory class for Provider model."""

    class Meta:  # noqa
        model = Provider

    name = factory.Faker('company')
    description = factory.Faker('text')


class DatasetTypeFactory(DjangoModelFactory):
    """Factory class for DatasetType model."""

    class Meta:  # noqa
        model = DatasetType

    name = factory.Faker('company')
    description = factory.Faker('text')
    type = CastType.HISTORICAL


class DatasetFactory(DjangoModelFactory):
    """Factory class for Dataset model."""

    class Meta:  # noqa
        model = Dataset

    name = factory.Faker('company')
    description = factory.Faker('text')
    type = factory.SubFactory(DatasetTypeFactory)
    time_step = DatasetTimeStep.DAILY
    store_type = DatasetStore.TABLE
    provider = factory.SubFactory(ProviderFactory)


class UnitFactory(DjangoModelFactory):
    """Factory class for Unitdel."""

    class Meta:  # noqa
        model = Unit

    name = factory.Sequence(
        lambda n: f'unit-{n}'
    )
    description = factory.Faker('text')


class AttributeFactory(DjangoModelFactory):
    """Factory class for Attribute model."""

    class Meta:  # noqa
        model = Attribute

    name = factory.Sequence(
        lambda n: f'attribute-{n}'
    )
    description = factory.Faker('text')
    variable_name = factory.Sequence(
        lambda n: f'var-{n}'
    )
    unit = factory.SubFactory(UnitFactory)


class DatasetAttributeFactory(DjangoModelFactory):
    """Factory class for DatasetAttribute model."""

    class Meta:  # noqa
        model = DatasetAttribute

    dataset = factory.SubFactory(DatasetFactory)
    attribute = factory.SubFactory(AttributeFactory)
    source = factory.Sequence(
        lambda n: f'attribute-{n}'
    )
    source_unit = factory.SubFactory(UnitFactory)


class CountryFactory(DjangoModelFactory):
    """Factory class for Country model."""

    class Meta:  # noqa
        model = Country

    name = factory.Faker('country')
    iso_a3 = factory.Faker('text')
    geometry = factory.LazyAttribute(
        lambda _: MultiPolygon(
            Polygon(((0, 0), (1, 0), (1, 1), (0, 1), (0, 0)))
        )
    )
    description = factory.Faker('text')


class DataSourceFileFactory(DjangoModelFactory):
    """Factory class for DataSourceFile model."""

    class Meta:  # noqa
        model = DataSourceFile

    name = factory.Faker('text')
    dataset = factory.SubFactory(DatasetFactory)
    start_date_time = factory.Faker('date_time')
    end_date_time = factory.Faker('date_time')
    created_on = factory.Faker('date_time')
    format = DatasetStore.NETCDF


class DataSourceFileCacheFactory(DjangoModelFactory):
    """Factory class for DataSourceFileCache model."""

    class Meta:  # noqa
        model = DataSourceFileCache

    source_file = factory.SubFactory(DataSourceFileFactory)
    hostname = factory.Faker('text')
    created_on = factory.Faker('date_time')


class VillageFactory(DjangoModelFactory):
    """Factory class for Village model."""

    class Meta:  # noqa
        model = Village

    name = factory.Sequence(
        lambda n: f'village-{n}'
    )
    description = factory.Faker('text')
