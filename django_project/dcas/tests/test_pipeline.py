# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline.
"""

from mock import patch, MagicMock
import pandas as pd
import dask.dataframe as dd

from dcas.models import DCASConfig, DCASConfigCountry
from dcas.pipeline import DCASDataPipeline
from dcas.tests.base import DCASPipelineBaseTest


def mock_function_do_nothing(df, *args, **kwargs):
    """Mock dask map_partitions do nothing."""
    return df


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
            self.farm_registry_group, self.request_date
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
            self.farm_registry_group, self.request_date
        )
        df = pipeline._merge_grid_data_with_config(df)
        self.assertIn('config_id', df.columns)
        expected_df = pd.DataFrame({
            'country_id': country_id_list,
            'config_id': [self.default_config.id, self.default_config.id]
        }, index=df.index)
        expected_df['config_id'] = expected_df['config_id'].astype('Int64')
        pd.testing.assert_frame_equal(df, expected_df)

    def test_process_farm_registry_data(self):
        """Test running process_farm_registry_data."""
        pipeline = DCASDataPipeline(
            self.farm_registry_group, self.request_date
        )
        pipeline.data_query.setup()

        grid_crop_meta_df = pd.DataFrame({
            'crop_id': [1],
            'crop_stage_type_id': [1],
            'planting_date': ['2025-01-01'],
            'grid_id': [1],
            'planting_date_epoch': [1],
            '__null_dask_index__': [0],
            'temperature': [10]
        })
        pipeline.data_query.read_grid_data_crop_meta_parquet = MagicMock(
            return_value=grid_crop_meta_df
        )
        pipeline.data_output.save = MagicMock()

        # Mock the map_partitions method
        with patch.object(
            dd.DataFrame, 'map_partitions', autospec=True
        ) as mock_map_partitions:
            # Set up the mock to call the mock function
            mock_map_partitions.side_effect = mock_function_do_nothing

            pipeline.process_farm_registry_data()

            mock_map_partitions.assert_called_once()

        pipeline.data_query.read_grid_data_crop_meta_parquet.\
            assert_called_once()
        pipeline.data_output.save.assert_called_once()
