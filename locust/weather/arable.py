# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Arable User Class for Locust Load Testing
"""

import datetime

from common.base_user import BaseUserScenario


class ArableWeatherUser(BaseUserScenario):
    """User scenario that accessing Arable ground observation data."""

    product_type = 'arable_ground_observation'
    attributes = [
        'total_evapotranspiration_flux',
        'max_relative_humidity',
        'max_day_temperature',
        'mean_relative_humidity',
        'mean_day_temperature',
        'min_relative_humidity',
        'min_day_temperature',
        'precipitation_total',
        'precipitation',
        'sea_level_pressure',
        'wind_heading',
        'wind_speed',
        'wind_speed_max',
        'wind_speed_min'
    ]

    # dates
    min_date = datetime.date(2024, 10, 1)
    max_date = datetime.datetime.now().date()
    min_rand_dates = 1
    max_rand_dates = 30
    default_dates = (
        min_date,
        min_date + datetime.timedelta(days=30),
    )

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'csv'
