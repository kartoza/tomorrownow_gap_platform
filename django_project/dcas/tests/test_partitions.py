# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Partitions functions.
"""

import pandas as pd

from dcas.tests.base import DCASPipelineBaseTest
from dcas.partitions import _merge_partition_gdd_config


class DCASPartitionsTest(DCASPipelineBaseTest):
    """DCAS Partitions test case."""

    def test_merge_partition_gdd_config(self):
        """Test merge_partition_gdd_config."""
        df = pd.DataFrame({
            'crop_id': [2, 10],  # Maize and Cassava
            'config_id': [self.default_config.id, self.default_config.id]
        })
        df = _merge_partition_gdd_config(df)
        self.assertIn('gdd_base', df.columns)
        self.assertIn('gdd_cap', df.columns)
        expected_df = pd.DataFrame({
            'crop_id': [2, 10],
            'config_id': [self.default_config.id, self.default_config.id],
            'gdd_base': [10, 12],
            'gdd_cap': [35, 35]
        })
        expected_df['gdd_base'] = expected_df['gdd_base'].astype('float64')
        expected_df['gdd_cap'] = expected_df['gdd_cap'].astype('float64')
        pd.testing.assert_frame_equal(df, expected_df)
