# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Class for Locust Load Testing
"""

import datetime
from locust import task

from common.api import Api, ApiWeatherGroupMode
from common.auth import auth_config
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
    max_rand_dates = 30

    # location
    point = (-1.404244, 35.008688,)

    def on_start(self):
        """Set up the test."""
        self.api = Api(
            self.client, auth_config.get_user(),
            weather_group_modes=[
                ApiWeatherGroupMode.BY_PRODUCT_TYPE,
                ApiWeatherGroupMode.BY_OUTPUT_TYPE,
                ApiWeatherGroupMode.BY_ATTRIBUTE_LENGTH
            ]
        )

    @task
    def random_variables(self):
        """Test with random variables and output types."""
        dates1 = (
            datetime.date(2012, 3, 1),
            datetime.date(2012, 3, 31),
        )

        # test with random variables
        self.api.weather(
            self.product_type,
            self.get_random_output_type(),
            self.get_random_attributes(),
            dates1[0],
            dates1[1],
            lat=self.point[0],
            lon=self.point[1]
        )
