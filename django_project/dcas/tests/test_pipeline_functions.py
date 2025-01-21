# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline functions.
"""

import numpy as np
import pandas as pd
from mock import patch

from dcas.tests.base import DCASPipelineBaseTest
from dcas.functions import calculate_growth_stage


def set_cache_dummy(cache_key, growth_stage_matrix, timeout):
    """Set cache mock."""
    pass


class DCASPipelineFunctionTest(DCASPipelineBaseTest):
    """DCAS Pipeline functions test case."""

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_empty_dict(self, mock_cache):
        """Test calculate_growth_stage from empty_dict."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 9999,
            'crop_stage_type_id': 9999,
            'prev_growth_stage_id': 111,
            'prev_growth_stage_start_date': 111,
            'gdd_sum_123': 9999,
            'config_id': 9999
        }
        epoch_list = [123]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 111)
        self.assertEqual(row['growth_stage_start_date'], 111)

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_no_change(self, mock_cache):
        """Test calculate_growth_stage no change from previous."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'prev_growth_stage_id': 2,
            'prev_growth_stage_start_date': 111,
            'gdd_sum_123': 440,
            'config_id': 1
        }
        epoch_list = [123]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 111)

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_find_using_threshold(self, mock_cache):
        """Test calculate_growth_stage no change from previous."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 420,
            'gdd_sum_124': 440,
            'gdd_sum_125': 450,
            'gdd_sum_126': 490,
            'config_id': 1
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

        # growth start date equals to the last item
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 410,
            'gdd_sum_124': 400,
            'gdd_sum_125': 420,
            'gdd_sum_126': 440,
            'config_id': 1
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 126)

        # growth start date equals to the only data
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 450,
            'config_id': 1
        }
        epoch_list = [
            123
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 123)

        # growth start date equals to planting date
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 124,
            'prev_growth_stage_id': None,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': np.nan,
            'gdd_sum_124': 440,
            'config_id': 1
        }
        epoch_list = [
            123,
            124
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

    @patch("dcas.service.cache")
    def test_calculate_growth_stage_na_value(self, mock_cache):
        """Test calculate_growth_stage no change from previous."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': pd.NA,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 420,
            'gdd_sum_124': 440,
            'gdd_sum_125': 450,
            'gdd_sum_126': 490
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)

        mock_cache.get.return_value = None
        mock_cache.set.side_effect = set_cache_dummy
        row = {
            'crop_id': 2,
            'crop_stage_type_id': 2,
            'planting_date_epoch': 123,
            'prev_growth_stage_id': np.nan,
            'prev_growth_stage_start_date': None,
            'gdd_sum_123': 420,
            'gdd_sum_124': 440,
            'gdd_sum_125': 450,
            'gdd_sum_126': 490
        }
        epoch_list = [
            123,
            124,
            125,
            126
        ]
        row = calculate_growth_stage(row, epoch_list)

        self.assertIn('growth_stage_id', row)
        self.assertIn('growth_stage_start_date', row)
        self.assertEqual(row['growth_stage_id'], 2)
        self.assertEqual(row['growth_stage_start_date'], 124)
