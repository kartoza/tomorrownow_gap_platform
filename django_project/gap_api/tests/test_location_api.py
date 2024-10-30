# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Location API.
"""

import datetime
import pytz
from mock import patch
from django.test import TestCase

from core.factories import UserF
from gap_api.models.location import Location
from gap_api.factories import LocationFactory
from gap_api.tasks.cleanup_location import cleanup_user_locations


class TestAPILocation(TestCase):
    """Test class for location API and task."""

    def setUp(self):
        """Set test class."""
        self.user = UserF.create()

    @patch('django.utils.timezone.now')
    def test_cleanup_user_locations(self, mock_timezone):
        """Test cleanup expired locations."""
        mock_timezone.return_value = datetime.datetime(
            2024, 10, 1, 0, 0, 0, tzinfo=pytz.utc
        )
        # expired_on will be assigned created_on+60days
        location1 = LocationFactory.create(
            user=self.user,
            created_on=datetime.datetime(
                2024, 7, 30, 0, 0, 0, tzinfo=pytz.utc
            )
        )
        location2 = LocationFactory.create(
            user=self.user,
            created_on=datetime.datetime(
                2024, 9, 1, 0, 0, 0, tzinfo=pytz.utc
            )
        )
        cleanup_user_locations()
        self.assertFalse(Location.objects.filter(
            id=location1.pk
        ).exists())
        self.assertTrue(Location.objects.filter(
            id=location2.pk
        ).exists())

    def test_upload_empty_name(self):
        """Test upload with empty name."""
        pass

    def test_upload_invalid_type(self):
        """Test upload with invalid file type."""
        pass

    def test_upload_invalid_shapefile(self):
        """Test upload with invalid shapefile."""
        pass

    def test_upload_invalid_crs(self):
        """Test upload with invalid crs."""
        pass

    def test_upload_invalid_geom_type(self):
        """Test upload with invalid geom type."""
        pass

    def test_upload_point(self):
        """Test upload with point."""
        pass

    def test_upload_polygons(self):
        """Test upload with polygons."""
        pass

    def test_upload_multipolygon(self):
        """Test upload with multipolygon."""
        pass
