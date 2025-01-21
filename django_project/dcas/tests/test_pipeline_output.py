# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Pipeline Output Test.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import dask.dataframe as dd
from dcas.outputs import DCASPipelineOutput, OutputType


class TestDCASPipelineOutput(unittest.TestCase):
    """Test DCAS Pipeline Output."""

    @patch("dcas.outputs.DCASPipelineOutput._upload_to_sftp")  # Mock SFTP
    @patch("pandas.DataFrame.to_csv")  # Mock CSV writing
    def test_save_message_output_csv(self, mock_to_csv, mock_sftp):
        """Test saving message output as CSV and uploading to SFTP."""

        # Create a dummy Dask DataFrame
        df = pd.DataFrame({"message": ["Test Message 1", "Test Message 2"]})
        dask_df = dd.from_pandas(df, npartitions=1)

        # Create instance of DCASPipelineOutput
        pipeline_output = DCASPipelineOutput(request_date="2025-01-15")

        # Run save method
        pipeline_output.save(OutputType.MESSAGE_DATA, dask_df)

        # Ensure `to_csv()` was called once
        mock_to_csv.assert_called_once()

        # Ensure `_upload_to_sftp()` was called with the correct file
        mock_sftp.assert_called_once_with(pipeline_output.csv_data_output_file)

    @patch("paramiko.SFTPClient.from_transport")
    @patch("paramiko.Transport", autospec=True)
    @patch("dcas.outputs.settings")
    def test_upload_to_sftp(
        self,
        mock_settings,
        mock_transport,
        mock_sftp_from_transport
    ):
        """Test that SFTP upload is triggered correctly."""

        # Set mock environment variables in `settings`
        mock_settings.SFTP_HOST = "127.0.0.1"
        mock_settings.SFTP_PORT = 2222
        mock_settings.SFTP_USERNAME = "user"
        mock_settings.SFTP_PASSWORD = "password"
        mock_settings.SFTP_REMOTE_PATH = "/home/user/upload/message_data.csv"

        # Mock the transport instance
        mock_transport_instance = mock_transport.return_value
        mock_transport_instance.connect.return_value = None

        # Mock the SFTP client and `put()` method
        mock_sftp_instance = MagicMock()
        mock_sftp_from_transport.return_value = mock_sftp_instance
        mock_sftp_instance.put.return_value = None

        # Initialize the pipeline output
        pipeline_output = DCASPipelineOutput(request_date="2025-01-15")

        # Create a temporary file for testing
        test_file = "/tmp/test_message_data.csv"
        with open(test_file, "w") as f:
            f.write("test message")

        # Call the method under test
        pipeline_output._upload_to_sftp(test_file)

        # Assertions
        # Verify `connect()` was called once
        mock_transport_instance.connect.assert_called_once_with(
            username=mock_settings.SFTP_USERNAME,
            password=mock_settings.SFTP_PASSWORD
        )

        # Verify `SFTPClient.from_transport()` was called once
        mock_sftp_from_transport.assert_called_once_with(
            mock_transport_instance
        )

        # Verify file was uploaded via `put()`
        mock_sftp_instance.put.assert_called_once_with(
            test_file, mock_settings.SFTP_REMOTE_PATH
        )

        # Verify SFTP client & transport were properly closed
        mock_sftp_instance.close.assert_called_once()
        mock_transport_instance.close.assert_called_once()
