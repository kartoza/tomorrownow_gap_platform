# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from factory.django import DjangoModelFactory

from core.factories import UserF
from gap.factories.farm import FarmFactory
from gap.factories.main import DatasetAttributeFactory
from gap.models import (
    Crop, Pest,
    FarmShortTermForecast, FarmShortTermForecastData,
    FarmProbabilisticWeatherForcast, FarmSuitablePlantingWindowSignal,
    FarmPlantingWindowTable, FarmPestManagement, FarmCropVariety,
    CropInsightRequest, CropStageType
)


class CropFactory(DjangoModelFactory):
    """Factory class for Crop model."""

    class Meta:  # noqa
        model = Crop

    name = factory.Sequence(
        lambda n: f'crop-{n}'
    )
    description = factory.Faker('text')


class PestFactory(DjangoModelFactory):
    """Factory class for Crop model."""

    class Meta:  # noqa
        model = Pest

    name = factory.Sequence(
        lambda n: f'pest-{n}'
    )
    description = factory.Faker('text')


class FarmShortTermForecastFactory(DjangoModelFactory):
    """Factory class for FarmShortTermForecast model."""

    class Meta:  # noqa
        model = FarmShortTermForecast

    farm = factory.SubFactory(FarmFactory)
    forecast_date = factory.Faker('date')


class FarmShortTermForecastDataFactory(DjangoModelFactory):
    """Factory class for FarmShortTermForecastData model."""

    class Meta:  # noqa
        model = FarmShortTermForecastData

    forecast = factory.SubFactory(FarmShortTermForecastFactory)
    dataset_attribute = factory.SubFactory(DatasetAttributeFactory)
    value_date = factory.Faker('date')
    value = factory.Faker('pyfloat')


class FarmProbabilisticWeatherForcastFactory(DjangoModelFactory):
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


class FarmSuitablePlantingWindowSignalFactory(DjangoModelFactory):
    """Factory class for FarmSuitablePlantingWindowSignal model."""

    class Meta:  # noqa
        model = FarmSuitablePlantingWindowSignal

    farm = factory.SubFactory(FarmFactory)
    generated_date = factory.Faker('date')
    signal = factory.Faker('text')


class FarmPlantingWindowTableFactory(DjangoModelFactory):
    """Factory class for FarmPlantingWindowTable model."""

    class Meta:  # noqa
        model = FarmPlantingWindowTable

    farm = factory.SubFactory(FarmFactory)
    recommendation_date = factory.Faker('date')
    recommended_date = factory.Faker('date')


class FarmPestManagementFactory(DjangoModelFactory):
    """Factory class for FarmPestManagement model."""

    class Meta:  # noqa
        model = FarmPestManagement

    farm = factory.SubFactory(FarmFactory)
    recommendation_date = factory.Faker('date')
    spray_recommendation = factory.Faker('text')


class FarmCropVarietyFactory(DjangoModelFactory):
    """Factory class for FarmCropVariety model."""

    class Meta:  # noqa
        model = FarmCropVariety

    farm = factory.SubFactory(FarmFactory)
    recommendation_date = factory.Faker('date')
    recommended_crop = factory.SubFactory(CropFactory)


class CropInsightRequestFactory(DjangoModelFactory):
    """Factory class for CropInsightRequest model."""

    class Meta:  # noqa
        model = CropInsightRequest

    unique_id = factory.Faker('uuid4')
    requested_by = factory.SubFactory(UserF)


class CropStageTypeFactory(DjangoModelFactory):
    """Factory class for CropStageType model."""

    class Meta:  # noqa
        model = CropStageType

    name = factory.Sequence(lambda n: f'name-{n}')
