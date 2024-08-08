# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Crop, Pest,
    FarmShortTermForecast, FarmProbabilisticWeatherForcastTable,
    FarmSuitablePlantingWindowSignal, FarmPlantingWindowTable,
    FarmPestManagement, FarmCropVariety
)


@admin.register(Crop)
class CropAdmin(AbstractDefinitionAdmin):
    """Crop admin."""

    pass


@admin.register(Pest)
class PestAdmin(AbstractDefinitionAdmin):
    """Pest admin."""

    pass


@admin.register(FarmShortTermForecast)
class FarmShortTermForecastAdmin(admin.ModelAdmin):
    """Admin for FarmShortTermForecast."""

    list_display = (
        'farm', 'forecast_date', 'attribute', 'value_date', 'value'
    )
    filter = ('farm', 'forecast_date', 'attribute')


@admin.register(FarmProbabilisticWeatherForcastTable)
class FarmProbabilisticWeatherForcastTableAdmin(admin.ModelAdmin):
    """Admin for FarmProbabilisticWeatherForcastTable."""

    list_display = (
        'farm', 'forecast_date', 'forecast_period'
    )
    filter = ('farm', 'forecast_date')


@admin.register(FarmSuitablePlantingWindowSignal)
class FarmSuitablePlantingWindowSignalAdmin(admin.ModelAdmin):
    """Admin for FarmSuitablePlantingWindowSignal."""

    list_display = (
        'farm', 'generated_date', 'signal'
    )
    filter = ('farm', 'generated_date')


@admin.register(FarmPlantingWindowTable)
class FarmPlantingWindowTableAdmin(admin.ModelAdmin):
    """Admin for FarmPlantingWindowTable."""

    list_display = (
        'farm', 'recommendation_date', 'recommended_date'
    )
    filter = ('farm', 'recommendation_date')


@admin.register(FarmPestManagement)
class FarmPestManagementAdmin(admin.ModelAdmin):
    """Admin for FarmPestManagement."""

    list_display = (
        'farm', 'recommendation_date', 'spray_recommendation'
    )
    filter = ('farm', 'recommendation_date')


@admin.register(FarmCropVariety)
class FarmCropVarietyAdmin(admin.ModelAdmin):
    """Admin for FarmCropVariety."""

    list_display = (
        'farm', 'recommendation_date', 'recommended_crop'
    )
    filter = ('farm', 'recommendation_date')
