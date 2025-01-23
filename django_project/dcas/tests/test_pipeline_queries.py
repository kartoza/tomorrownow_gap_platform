# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Queries functions.
"""

from mock import patch, MagicMock
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
