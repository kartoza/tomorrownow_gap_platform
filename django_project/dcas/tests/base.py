# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base test for DCAS Pipeline.
"""

import datetime
from django.test import TestCase

from gap.factories import (
    FarmRegistryGroupFactory, FarmRegistryFactory,
    FarmFactory, GridFactory, CountryFactory
)
from dcas.models import DCASConfig


class DCASPipelineBaseTest(TestCase):
    """DCAS Pipeline base test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json',
        '1.dcas_config.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '14.crop.json',
        '15.gdd_config.json',
        '16.gdd_matrix.json'
    ]

    def setUp(self):
        """Set DCASPipelineBaseTest class."""
        country_1 = CountryFactory(
            iso_a3='KE'
        )
        country_2 = CountryFactory(
            iso_a3='ZAF'
        )
        self.grid_1 = GridFactory(
            country=country_1
        )
        self.grid_2 = GridFactory(
            country=country_2
        )

        self.default_config = DCASConfig.objects.get(id=1)
        self.request_date = datetime.date(2025, 1, 15)

        # create farm registries
        self.farm_registry_group = FarmRegistryGroupFactory()
        self._create_farm_registry_for_grid(
            self.farm_registry_group, self.grid_1)
        self._create_farm_registry_for_grid(
            self.farm_registry_group, self.grid_2)

    def _create_farm_registry_for_grid(self, group, grid):
        farm = FarmFactory(grid=grid)
        return FarmRegistryFactory(
            group=group,
            farm=farm
        )
