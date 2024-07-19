# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from django.contrib.gis.geos import Point, MultiPolygon, Polygon

from core.factories import BaseMetaFactory, BaseFactory
from gap.models import (
    CastType,
    DatasetType,
    Dataset,
    Provider,
    Unit,
    Attribute,
    DatasetAttribute,
    Country,
    Station,
    Measurement,
    ObservationType,
    DatasetTimeStep,
    DatasetStore,
    DataSourceFile
)


class ProviderFactory(
    BaseFactory[Provider], metaclass=BaseMetaFactory[Provider]
):
    """Factory class for Provider model."""

    class Meta:  # noqa
        model = Provider

    name = factory.Faker('company')
    description = factory.Faker('text')


class DatasetTypeFactory(
    BaseFactory[DatasetType], metaclass=BaseMetaFactory[DatasetType]
):
    """Factory class for DatasetType model."""

    class Meta:  # noqa
        model = DatasetType

    name = factory.Faker('company')
    description = factory.Faker('text')
    type = CastType.HISTORICAL


class DatasetFactory(
    BaseFactory[Dataset], metaclass=BaseMetaFactory[Dataset]
):
    """Factory class for Dataset model."""

    class Meta:  # noqa
        model = Dataset

    name = factory.Faker('company')
    description = factory.Faker('text')
    type = factory.SubFactory(DatasetTypeFactory)
    time_step = DatasetTimeStep.DAILY
    store_type = DatasetStore.TABLE
    provider = factory.SubFactory(ProviderFactory)


class UnitFactory(
    BaseFactory[Unit], metaclass=BaseMetaFactory[Unit]
):
    """Factory class for Unitdel."""

    class Meta:  # noqa
        model = Unit

    name = factory.Sequence(
        lambda n: f'unit-{n}'
    )
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
    variable_name = factory.Sequence(
        lambda n: f'var-{n}'
    )
    unit = factory.SubFactory(UnitFactory)


class DatasetAttributeFactory(
    BaseFactory[DatasetAttribute], metaclass=BaseMetaFactory[DatasetAttribute]
):
    """Factory class for DatasetAttribute model."""

    class Meta:  # noqa
        model = DatasetAttribute

    dataset = factory.SubFactory(DatasetFactory)
    attribute = factory.SubFactory(AttributeFactory)
    source = factory.Sequence(
        lambda n: f'attribute-{n}'
    )
    source_unit = factory.SubFactory(UnitFactory)


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
    iso_a3 = factory.Faker('text')
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
    code = factory.Sequence(
        lambda n: f'code-{n}'
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
    dataset_attribute = factory.SubFactory(DatasetAttributeFactory)
    date_time = factory.Faker('date_time')
    value = factory.Faker('pyfloat')


class DataSourceFileFactory(
    BaseFactory[DataSourceFile], metaclass=BaseMetaFactory[DataSourceFile]
):
    """Factory class for DataSourceFile model."""

    class Meta:  # noqa
        model = DataSourceFile

    name = factory.Faker('text')
    dataset = factory.SubFactory(DatasetFactory)
    start_date_time = factory.Faker('date_time')
    end_date_time = factory.Faker('date_time')
    created_on = factory.Faker('date_time')
    format = DatasetStore.NETCDF
