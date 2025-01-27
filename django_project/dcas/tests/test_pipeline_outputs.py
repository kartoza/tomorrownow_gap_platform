# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Outputs functions.
"""

import csv
import os
import datetime
from mock import patch, MagicMock

from dcas.tests.base import DCASPipelineBaseTest
from dcas.outputs import DCASPipelineOutput


class DCASOutputsTest(DCASPipelineBaseTest):
    """DCAS Outputs test case."""

    @patch('dcas.outputs.duckdb.connect')
    def test_convert_to_csv(self, mock_duckdb_connect):
        """Test convert_to_csv function."""
        # Mock the connection object
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn
        data_output = DCASPipelineOutput(datetime.date(2025, 1, 1))
        data_output._setup_s3fs()
        os.makedirs(data_output.TMP_BASE_DIR, exist_ok=True)
        with open(
            os.path.join(data_output.TMP_BASE_DIR, 'output_20250101.csv'),
            'w',
            newline=''
        ) as file:
            writer = csv.writer(file)
            field = ["farmerId", "crop", "plantingDate"]
            writer.writerow(field)

        csv_file = data_output.convert_to_csv()

        mock_duckdb_connect.assert_called_once()
        mock_conn.install_extension.assert_any_call("httpfs")
        mock_conn.load_extension.assert_any_call("httpfs")
        mock_conn.install_extension.assert_any_call("spatial")
        mock_conn.load_extension.assert_any_call("spatial")
        mock_conn.sql.assert_called_once()
        mock_conn.close.assert_called_once()
        self.assertIn('output_20250101.csv', csv_file)
