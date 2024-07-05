# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.contrib.gis.db import models

from core.models.common import Definition
from gap.models.common import Provider


class DatasetType:
    """Dataset type."""

    CLIMATE_REANALYSIS = 'Climate Reanalysis'
    SHORT_TERM_FORECAST = 'Short-term Forecast'
    SEASONAL_FORECAST = 'Seasonal Forecast'
    GROUND_OBSERVATIONAL = 'Ground Observational'
    AIRBORNE_OBSERVATIONAL = 'Airborne Observational'


class DatasetStore:
    """Dataset Storing Type."""

    TABLE = 'TABLE'
    NETCDF = 'NETCDF'
    EXT_API = 'EXT_API'


class DatasetTimeStep:
    """Dataset Time Step."""

    HOURLY = 'HOURLY'
    DAILY = 'DAILY'


class Dataset(Definition):
    """Model representing dataset of measument collection."""

    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    type = models.CharField(
        choices=(
            (DatasetType.CLIMATE_REANALYSIS, DatasetType.CLIMATE_REANALYSIS),
            (DatasetType.SHORT_TERM_FORECAST, DatasetType.SHORT_TERM_FORECAST),
            (DatasetType.SEASONAL_FORECAST, DatasetType.SEASONAL_FORECAST),
            (
                DatasetType.GROUND_OBSERVATIONAL,
                DatasetType.GROUND_OBSERVATIONAL
            ),
            (
                DatasetType.AIRBORNE_OBSERVATIONAL,
                DatasetType.AIRBORNE_OBSERVATIONAL
            ),
        ),
        max_length=512
    )
    time_step = models.CharField(
        choices=(
            (DatasetTimeStep.DAILY, DatasetTimeStep.DAILY),
            (DatasetTimeStep.HOURLY, DatasetTimeStep.HOURLY),
        ),
        max_length=512
    )
    store_type = models.CharField(
        choices=(
            (DatasetStore.TABLE, DatasetStore.TABLE),
            (DatasetStore.NETCDF, DatasetStore.NETCDF),
            (DatasetStore.EXT_API, DatasetStore.EXT_API),
        ),
        max_length=512
    )
