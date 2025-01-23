# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Queries functions.
"""

from mock import patch, MagicMock

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
        data_query = DataQuery()

        data_query.read_grid_data_crop_meta_parquet('/tmp/dcas/grid_crop')
        mock_duckdb_connect.assert_called_once()
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()
