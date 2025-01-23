# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Queries functions.
"""

from mock import patch, MagicMock
import pandas as pd

from dcas.tests.base import DCASPipelineBaseTest
from dcas.queries import DataQuery


class DCASQueriesTest(DCASPipelineBaseTest):
    """DCAS Queries test case."""

    @patch('dcas.queries.duckdb.connect')
    def test_read_grid_data_crop_meta_parquet(self, mock_duckdb_connect):
        """Test read_grid_data_crop_meta_parquet function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        data_query = DataQuery('')

        data_query.read_grid_data_crop_meta_parquet('/tmp/dcas/grid_crop')
        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("dcas.queries.duckdb.connect")
    def test_get_farms_without_messages(self, mock_duckdb_connect):
        """Test retrieving farms with missing messages."""
        # Mock DuckDB return DataFrame
        mock_df = pd.DataFrame({
            'farm_id': [1, 2, 3, 4],
            'crop_id': [101, 102, 103, 104],
            'message': ["MSG1", None, "MSG3", None],
            'message_2': [None, None, None, None],
            'message_3': [None, None, None, None],
            'message_4': [None, None, None, "MSG4"],
            'message_5': [None, None, None, None]
        })

        expected_df = mock_df[
            mock_df[
                [
                    'message',
                    'message_2',
                    'message_3',
                    'message_4',
                    'message_5'
                ]
            ].isnull().all(axis=1)][['farm_id', 'crop_id']]

        # Configure mock connection
        mock_conn = mock_duckdb_connect.return_value
        mock_conn.sql.return_value.df.return_value = expected_df

        # Call the function
        result_df = DataQuery.get_farms_without_messages(
            "/tmp/dcas/farm_crop.parquet"
        )

        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertEqual(len(result_df), len(expected_df))
        self.assertEqual(list(result_df.columns), ['farm_id', 'crop_id'])

        # Ensure the DataFrames are equal
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Ensure DuckDB was called correctly
        expected_query = """
            SELECT farm_id, crop_id
            FROM read_parquet('/tmp/dcas/farm_crop.parquet')
            WHERE message IS NULL
            AND message_2 IS NULL
            AND message_3 IS NULL
            AND message_4 IS NULL
            AND message_5 IS NULL
        """
        actual_query = mock_conn.sql.call_args[0][0]

        # Strip unnecessary spaces and line breaks before comparing
        normalized_expected_query = " ".join(expected_query.split())
        normalized_actual_query = " ".join(actual_query.split())

        self.assertEqual(normalized_actual_query, normalized_expected_query)

        mock_conn.close.assert_called_once()
