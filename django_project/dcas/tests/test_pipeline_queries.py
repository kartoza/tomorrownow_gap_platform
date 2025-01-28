# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Queries functions.
"""

import re
from mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine

from dcas.tests.base import DCASPipelineBaseTest
from dcas.pipeline import DCASDataPipeline
from dcas.queries import DataQuery


class DCASQueriesTest(DCASPipelineBaseTest):
    """DCAS Queries test case."""

    @patch('dcas.queries.duckdb.connect')
    def test_read_grid_data_crop_meta_parquet(self, mock_duckdb_connect):
        """Test read_grid_data_crop_meta_parquet function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        data_query = DataQuery()

        data_query.read_grid_data_crop_meta_parquet('/tmp/dcas/grid_crop')
        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("dcas.queries.duckdb.connect")
    def test_get_farms_without_messages_chunked(self, mock_duckdb_connect):
        """Test retrieving farms with missing messages in chunks."""
        # Mock DuckDB return DataFrames (Simulating chunked retrieval)
        chunk_1 = pd.DataFrame({'farm_id': [1, 2], 'crop_id': [101, 102]})
        chunk_2 = pd.DataFrame({'farm_id': [3, 4], 'crop_id': [103, 104]})

        expected_chunks = [chunk_1, chunk_2]

        # Configure mock connection to return chunks in order
        mock_conn = mock_duckdb_connect.return_value
        mock_conn.sql.return_value.df.side_effect = expected_chunks

        # Call the function
        result_chunks = list(
            DataQuery.get_farms_without_messages(
                "/tmp/dcas/farm_crop.parquet", chunk_size=2
            )
        )

        # Ensure we receive correct number of chunks
        self.assertEqual(len(result_chunks), len(expected_chunks))

        # Validate each chunk
        for result_df, expected_df in zip(result_chunks, expected_chunks):
            pd.testing.assert_frame_equal(result_df, expected_df)

        # Check DuckDB Query
        expected_query_pattern = re.compile(
            r"SELECT farm_id, crop_id "
            r"FROM read_parquet\('/tmp/dcas/farm_crop.parquet'\) "
            r"WHERE message IS NULL "
            r"AND message_2 IS NULL "
            r"AND message_3 IS NULL "
            r"AND message_4 IS NULL "
            r"AND message_5 IS NULL"
            r"(\s+LIMIT\s+\d+\s+OFFSET\s+\d+)?"
        )

        actual_query = " ".join(mock_conn.sql.call_args[0][0].split())

        # Assert query structure matches, ignoring chunking additions
        self.assertRegex(actual_query, expected_query_pattern)

        mock_conn.close.assert_called_once()

    def test_grid_data_with_crop_meta(self):
        """Test grid_data_with_crop_meta functions."""
        pipeline = DCASDataPipeline(
            self.farm_registry_group, self.request_date
        )
        conn_engine = create_engine(pipeline._conn_str())
        pipeline.data_query.setup(conn_engine)
        df = pipeline.data_query.grid_data_with_crop_meta(
            self.farm_registry_group
        )
        self.assertIn('crop_id', df.columns)
        self.assertIn('crop_stage_type_id', df.columns)
        self.assertIn('grid_id', df.columns)
        self.assertIn('grid_crop_key', df.columns)
        conn_engine.dispose()
