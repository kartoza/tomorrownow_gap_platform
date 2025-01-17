# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline.
"""

import pandas as pd
import datetime
from django.test import TestCase

from dcas.models import DCASConfig, DCASConfigCountry
from dcas.pipeline import DCASDataPipeline
from gap.factories import (
    FarmRegistryGroupFactory, FarmRegistryFactory,
    FarmFactory, GridFactory, CountryFactory
)


class DCASPipelineTest(TestCase):
    """DCAS Pipeline test case."""

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
        """Set DCASPipelineTest class."""
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

    def test_merge_grid_data_with_config(self):
        """Test merge_grid_data_with_config."""
        id_list = [self.grid_1.id, self.grid_2.id]
        country_id_list = [
            self.grid_1.country.id,
            self.grid_2.country.id,
        ]
        df = pd.DataFrame({
            'country_id': country_id_list
        }, index=id_list)
        # create config for country 2
        config = DCASConfig.objects.create(name='config')
        DCASConfigCountry.objects.create(
            config=config,
            country=self.grid_2.country
        )

        pipeline = DCASDataPipeline(
            self.farm_registry_group,
            config, self.request_date
        )
        df = pipeline._merge_grid_data_with_config(df)
        self.assertIn('config_id', df.columns)
        expected_df = pd.DataFrame({
            'country_id': country_id_list,
            'config_id': [self.default_config.id, config.id]
        }, index=df.index)
        expected_df['config_id'] = expected_df['config_id'].astype('Int64')
        pd.testing.assert_frame_equal(df, expected_df)

    def test_merge_grid_data_with_config_using_default(self):
        """Test merge_grid_data_with_config using default."""
        id_list = [self.grid_1.id, self.grid_2.id]
        country_id_list = [
            self.grid_1.country.id,
            self.grid_2.country.id,
        ]
        df = pd.DataFrame({
            'country_id': country_id_list
        }, index=id_list)
        pipeline = DCASDataPipeline(
            self.farm_registry_group,
            self.default_config, self.request_date
        )
        df = pipeline._merge_grid_data_with_config(df)
        self.assertIn('config_id', df.columns)
        expected_df = pd.DataFrame({
            'country_id': country_id_list,
            'config_id': [self.default_config.id, self.default_config.id]
        }, index=df.index)
        expected_df['config_id'] = expected_df['config_id'].astype('Int64')
        pd.testing.assert_frame_equal(df, expected_df)
