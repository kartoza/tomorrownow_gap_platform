# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase

from gap.factories import StationHistoryFactory
from gap.models import StationHistory


class StationHistoryCRUDTest(TestCase):
    """StationHistory test case."""

    Factory = StationHistoryFactory
    Model = StationHistory

    def test_create_object(self):
        """Test create object."""
        obj = self.Factory()
        self.assertIsInstance(obj, self.Model)
        self.assertTrue(self.Model.objects.filter(id=obj.id).exists())

    def test_read_object(self):
        """Test read object."""
        obj = self.Factory()
        fetched_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_object(self):
        """Test update object."""
        obj = self.Factory()
        new_altitude = 100.0
        obj.altitude = new_altitude
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.altitude, new_altitude)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())
