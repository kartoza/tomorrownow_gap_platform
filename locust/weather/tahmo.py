# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo User Class for Locust Load Testing
"""

import datetime

from common.base_user import BaseUserScenario


class TahmoWeatherUser(BaseUserScenario):
    """User scenario that accessing Tahmo ground observation data."""

    product_type = 'tahmo_ground_observation'
    attributes = [
        'precipitation',
        'solar_radiation',
        'max_relative_humidity',
        'min_relative_humidity',
        'average_air_temperature',
        'max_air_temperature',
        'min_air_temperature'
    ]

    # dates
    min_date = datetime.date(2019, 1, 1)
    max_date = datetime.date(2023, 12, 31)
    min_rand_dates = 1
    max_rand_dates = 30
    default_dates = (
        datetime.date(2019, 3, 1),
        datetime.date(2019, 3, 15),
    )

    # location
    point = (-1.404244, 35.008688,)

    # output
    default_output_type = 'csv'
