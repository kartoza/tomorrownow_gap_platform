# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo User Class for Locust Load Testing
"""

import datetime
from locust import task

from common.api import Api, ApiWeatherGroupMode
from common.auth import auth_config
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

    # location
    point = (-1.404244, 35.008688,)

    def on_start(self):
        """Set up the test."""
        self.api = Api(
            self.client, auth_config.get_user()
        )

    @task
    def random_variables(self):
        """Test with random variables and output types."""
        group_modes = [
            ApiWeatherGroupMode.BY_PRODUCT_TYPE,
            ApiWeatherGroupMode.BY_OUTPUT_TYPE,
            ApiWeatherGroupMode.BY_ATTRIBUTE_LENGTH
        ]
        dates1 = (
            datetime.date(2019, 3, 1),
            datetime.date(2019, 3, 15),
        )

        # test with random variables
        self.api.weather(
            self.product_type,
            self.get_random_output_type(),
            self.get_random_attributes(),
            dates1[0],
            dates1[1],
            lat=self.point[0],
            lon=self.point[1],
            group_modes=group_modes
        )
