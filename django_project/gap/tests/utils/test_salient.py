# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Salient Utilities.
"""


import unittest
from unittest.mock import patch, Mock, mock_open
import requests
import pandas as pd

from gap.utils.salient import patch_download_query


class TestPatchDownloadQuery(unittest.TestCase):
    """Test patch_download_query functino."""

    @patch("salientsdk.login_api.get_current_session")
    @patch("salientsdk.login_api.get_verify_ssl")
    @patch("requests.Session.get")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_download_success(
        self, mock_exists, mocked_open, mock_get, mock_verify_ssl,
        mock_get_current_session
    ):
        """Test success download."""
        # Set up the mocks
        mock_exists.return_value = False
        mock_response = Mock()
        mock_response.iter_content = Mock(
            return_value=["data_chunk1", "data_chunk2"])
        mock_response.raise_for_status = Mock()
        mock_get.return_value.__enter__.return_value = mock_response
        mock_get_current_session.return_value = requests.Session()
        mock_verify_ssl.return_value = True

        # Call the function
        file_name = patch_download_query(
            "http://example.com/data", "data.nc", verbose=True, format="nc")

        # Assert the expected behavior
        mock_get.assert_called_once_with(
            "http://example.com/data", verify=True, stream=True)
        mocked_open.assert_called_once_with("data.nc", "wb")
        handle = mocked_open()
        handle.write.assert_any_call("data_chunk1")
        handle.write.assert_any_call("data_chunk2")
        self.assertEqual(file_name, "data.nc")

    @patch("salientsdk.login_api.get_current_session")
    @patch("salientsdk.login_api.get_verify_ssl")
    @patch("os.path.exists")
    def test_skip_download_if_exists(
        self, mock_exists, mock_verify_ssl, mock_get_current_session
    ):
        """Test skip download if exists."""
        # Set up the mocks
        mock_exists.return_value = True
        mock_get_current_session.return_value = requests.Session()
        mock_verify_ssl.return_value = True

        # Call the function
        file_name = patch_download_query(
            "http://example.com/data", "data.txt", verbose=True)

        # Assert the expected behavior
        mock_exists.assert_called_once_with("data.txt")
        self.assertEqual(file_name, "data.txt")

    @patch("salientsdk.login_api.get_current_session")
    @patch("salientsdk.login_api.get_verify_ssl")
    @patch("requests.Session.get")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_download_strict_false_on_error(
        self, mock_exists, mocked_open, mock_get, mock_verify_ssl,
        mock_get_current_session
    ):
        """Test download with strict false."""
        # Set up the mocks
        mock_exists.return_value = False
        mock_response = Mock()
        mock_response.iter_content = Mock(return_value=[b"data_chunk"])
        mock_response.raise_for_status.side_effect = (
            requests.HTTPError("Error")
        )
        mock_get.return_value.__enter__.return_value = mock_response
        mock_get_current_session.return_value = requests.Session()
        mock_verify_ssl.return_value = True

        # Call the function with strict=False
        file_name = patch_download_query(
            "http://example.com/data", "data.txt", strict=False)

        # Assert the expected behavior
        self.assertEqual(str(file_name), str(pd.NA))

    @patch("salientsdk.login_api.get_current_session")
    @patch("salientsdk.login_api.get_verify_ssl")
    @patch("requests.Session.get")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_handle_different_formats(
        self, mock_exists, mock_open, mock_get, mock_verify_ssl,
        mock_get_current_session
    ):
        """Test download with different formats."""
        # Set up the mocks
        mock_exists.return_value = False
        mock_response = Mock()
        mock_response.iter_content = Mock(return_value=[b"data_chunk"])
        mock_response.raise_for_status = Mock()
        mock_get.return_value.__enter__.return_value = mock_response
        mock_get_current_session.return_value = requests.Session()
        mock_verify_ssl.return_value = True

        # Call the function with different format
        file_name_nc = patch_download_query(
            "http://example.com/data", "data.nc")

        # Assert the expected behavior for .nc format
        mock_open.assert_called_with("data.nc", "wb")
        self.assertEqual(file_name_nc, "data.nc")

        # Reset the mock_open
        mock_open.reset_mock()

        # Call the function with a non-.nc format
        file_name_txt = patch_download_query(
            "http://example.com/data", "data.txt")

        # Assert the expected behavior for .txt format
        mock_open.assert_called_with("data.txt", "w")
        self.assertEqual(file_name_txt, "data.txt")
