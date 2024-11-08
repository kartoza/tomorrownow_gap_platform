# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Class for Locust Load Testing
"""

import datetime

from common.base_user import BaseUserScenario


class CBAMWeatherUser(BaseUserScenario):
    """User scenario that accessing CBAM historical data."""

    product_type = 'cbam_historical_analysis'
    attributes = [
        'min_temperature',
        'min_day_temperature',
        'total_rainfall',
        'max_day_temperature',
        'min_night_temperature',
        'total_solar_irradiance',
        'average_solar_irradiance',
        'max_night_temperature',
        'max_temperature',
        'total_evapotranspiration_flux'
    ]

    # dates
    min_date = datetime.date(2013, 1, 1)
    max_date = datetime.date(2023, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 90

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'netcdf'


class CBAMBiasAdjustWeatherUser(BaseUserScenario):
    """User scenario that accessing CBAM Bias Adjust historical data."""

    product_type = 'cbam_historical_analysis_bias_adjust'
    attributes = [
        'min_temperature',
        'total_rainfall',
        'total_solar_irradiance',
        'max_temperature'
    ]

    # dates
    min_date = datetime.date(2013, 1, 1)
    max_date = datetime.date(2023, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 90

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'netcdf'


class CBAMShortTermWeatherUser(BaseUserScenario):
    """User scenario that accessing CBAM Short-term forecast data."""

    product_type = 'cbam_shortterm_forecast'
    attributes = [
        'total_rainfall',
        'total_evapotranspiration_flux',
        'max_temperature',
        'min_temperature',
        'precipitation_probability',
        'humidity_maximum',
        'humidity_minimum',
        'wind_speed_avg',
        'solar_radiation'
    ]

    # dates
    min_date = datetime.datetime.now().date()
    max_date = (
        datetime.datetime.now().date() + datetime.timedelta(days=14)
    )
    min_rand_dates = 1
    max_rand_dates = 14
    default_dates = (
        min_date,
        max_date,
    )

    # location
    point = (-0.02378, 35.008688,)

    # output
    default_output_type = 'netcdf'
