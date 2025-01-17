# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline.
"""

import pandas as pd

from dcas.models import DCASConfig, DCASConfigCountry
from dcas.pipeline import DCASDataPipeline
from dcas.tests.base import DCASPipelineBaseTest


class DCASPipelineTest(DCASPipelineBaseTest):
    """DCAS Pipeline test case."""

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
