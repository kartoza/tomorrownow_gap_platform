# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Disdrometer User Class for Locust Load Testing
"""

import datetime

from common.base_user import BaseUserScenario


class DisdrometerWeatherUser(BaseUserScenario):
    """User scenario that accessing Disdrometer data."""

    product_type = 'disdrometer_ground_observation'
    attributes = [
        'atmospheric_pressure',
        'depth_of_water',
        'electrical_conductivity_of_precipitation',
        'electrical_conductivity_of_water',
        'lightning_distance',
        'shortwave_radiation',
        'soil_moisture_content',
        'soil_temperature',
        'surface_air_temperature',
        'wind_speed',
        'wind_gusts',
        'precipitation_total',
        'precipitation',
        'relative_humidity',
        'wind_heading'
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
    point = (0.00271, 34.596908,)

    # output
    default_output_type = 'csv'
