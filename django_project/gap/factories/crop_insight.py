# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory

from core.factories import BaseMetaFactory, BaseFactory
from gap.factories.farm import FarmFactory
from gap.factories.main import DatasetAttributeFactory
from gap.models import (
    Crop, Pest,
    FarmShortTermForecast, FarmProbabilisticWeatherForcast,
    FarmSuitablePlantingWindowSignal,
    FarmPlantingWindowTable, FarmPestManagement, FarmCropVariety
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


class PestFactory(
    BaseFactory[Pest], metaclass=BaseMetaFactory[Pest]
):
    """Factory class for Crop model."""

    class Meta:  # noqa
        model = Pest

    name = factory.Sequence(
        lambda n: f'pest-{n}'
    )
    description = factory.Faker('text')


class FarmShortTermForecastFactory(
    BaseFactory[FarmShortTermForecast],
    metaclass=BaseMetaFactory[FarmShortTermForecast]
):
    """Factory class for FarmShortTermForecast model."""

    class Meta:  # noqa
        model = FarmShortTermForecast

    farm = factory.SubFactory(FarmFactory)
    forecast_date = factory.Faker('date')
    attribute = factory.SubFactory(DatasetAttributeFactory)
    value_date = factory.Faker('date')
    value = factory.Faker('pyfloat')


class FarmProbabilisticWeatherForcastFactory(
    BaseFactory[FarmProbabilisticWeatherForcast],
    metaclass=BaseMetaFactory[FarmProbabilisticWeatherForcast]
):
    """Factory class for FarmProbabilisticWeatherForcast model."""

    class Meta:  # noqa
        model = FarmProbabilisticWeatherForcast

    farm = factory.SubFactory(FarmFactory)
    forecast_date = factory.Faker('date')
    forecast_period = factory.Faker('text')
    temperature_10th_percentile = factory.Faker('pyfloat')
    temperature_50th_percentile = factory.Faker('pyfloat')
    temperature_90th_percentile = factory.Faker('pyfloat')
    precipitation_10th_percentile = factory.Faker('pyfloat')
    precipitation_50th_percentile = factory.Faker('pyfloat')
    precipitation_90th_percentile = factory.Faker('pyfloat')


class FarmSuitablePlantingWindowSignalFactory(
    BaseFactory[FarmSuitablePlantingWindowSignal],
    metaclass=BaseMetaFactory[FarmSuitablePlantingWindowSignal]
):
    """Factory class for FarmSuitablePlantingWindowSignal model."""

    class Meta:  # noqa
        model = FarmSuitablePlantingWindowSignal

    farm = factory.SubFactory(FarmFactory)
    generated_date = factory.Faker('date')
    signal = factory.Faker('text')


class FarmPlantingWindowTableFactory(
    BaseFactory[FarmPlantingWindowTable],
    metaclass=BaseMetaFactory[FarmPlantingWindowTable]
):
    """Factory class for FarmPlantingWindowTable model."""

    class Meta:  # noqa
        model = FarmPlantingWindowTable

    farm = factory.SubFactory(FarmFactory)
    recommendation_date = factory.Faker('date')
    recommended_date = factory.Faker('date')


class FarmPestManagementFactory(
    BaseFactory[FarmPestManagement],
    metaclass=BaseMetaFactory[FarmPestManagement]
):
    """Factory class for FarmPestManagement model."""

    class Meta:  # noqa
        model = FarmPestManagement

    farm = factory.SubFactory(FarmFactory)
    recommendation_date = factory.Faker('date')
    spray_recommendation = factory.Faker('text')


class FarmCropVarietyFactory(
    BaseFactory[FarmCropVariety],
    metaclass=BaseMetaFactory[FarmCropVariety]
):
    """Factory class for FarmCropVariety model."""

    class Meta:  # noqa
        model = FarmCropVariety

    farm = factory.SubFactory(FarmFactory)
    recommendation_date = factory.Faker('date')
    recommended_crop = factory.SubFactory(CropFactory)
