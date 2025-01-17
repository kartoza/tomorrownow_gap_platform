# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for APIRequestLogResource.
"""

from rest_framework.test import APITestCase
from gap_api.models import APIRequestLog
from gap_api.resources import APIRequestLogResource
from datetime import datetime


class APIRequestLogResourceTest(APITestCase):
    """Test case for APIRequestLogResource."""

    def setUp(self):
        """Set up a test APIRequestLog instance."""
        self.api_log = APIRequestLog.objects.create(
            user=None,
            query_params={
                "product": "WeatherData",
                "attributes": ["temp", "humidity"],
                "output_type": "CSV",
                "start_date": "2025-01-01",
                "end_date": "2025-01-15",
            },
            requested_at=datetime(2025, 1, 15, 12, 30, 0),
            response_ms=200,
            status_code=200,
            view_method="GET",
            path="/api/data/",
            remote_addr="127.0.0.1",
            host="localhost",
        )
        self.resource = APIRequestLogResource()

    def test_dehydrate_product(self):
        """Test extraction of 'product' from query_params."""
        result = self.resource.dehydrate_product(self.api_log)
        self.assertEqual(result, "WeatherData")

    def test_dehydrate_attributes(self):
        """Test extraction of 'attributes' from query_params."""
        result = self.resource.dehydrate_attributes(self.api_log)
        self.assertEqual(result, ["temp", "humidity"])

    def test_dehydrate_output_type(self):
        """Test extraction of 'output_type' from query_params."""
        result = self.resource.dehydrate_output_type(self.api_log)
        self.assertEqual(result, "CSV")

    def test_dehydrate_start_date(self):
        """Test extraction of 'start_date' from query_params."""
        result = self.resource.dehydrate_start_date(self.api_log)
        self.assertEqual(result, "2025-01-01")

    def test_dehydrate_end_date(self):
        """Test extraction of 'end_date' from query_params."""
        result = self.resource.dehydrate_end_date(self.api_log)
        self.assertEqual(result, "2025-01-15")

    def test_invalid_key_returns_empty_string(self):
        """Test that an invalid key returns an empty string."""
        result = self.resource._extract_from_query_params(
            self.api_log, "invalid_key")
        self.assertEqual(result, "")
