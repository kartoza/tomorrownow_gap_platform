# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient User Class for Locust Load Testing
"""

import datetime

from common.base_user import BaseUserScenario


class SalientWeatherUser(BaseUserScenario):
    """User scenario that accessing Salient forecast data."""

    product_type = 'salient_seasonal_forecast'
    attributes = [
        'temperature',
        'temperature_clim',
        'temperature_anom',
        'precipitation',
        'precipitation_clim',
        'precipitation_anom',
        'min_temperature',
        'min_temperature_clim',
        'min_temperature_anom',
        'max_temperature',
        'max_temperature_clim',
        'max_temperature_anom',
        'relative_humidty',
        'relative_humidty_clim',
        'relative_humidty_anom',
        'solar_radiation',
        'solar_radiation_clim',
        'solar_radiation_anom',
        'wind_speed',
        'wind_speed_clim',
        'wind_speed_anom',
    ]

    # dates
    min_date = datetime.datetime.now().date()
    max_date = (
        datetime.datetime.now().date() + datetime.timedelta(days=90)
    )
    min_rand_dates = 1
    max_rand_dates = 90
    default_dates = (
        min_date,
        max_date,
    )

    # location
    point = (-0.625, 33.38,)

    # output
    default_output_type = 'netcdf'



