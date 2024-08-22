# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for preferences.
"""

from django.contrib.gis.geos import Polygon
from django.db.utils import IntegrityError
from django.test import TestCase

from gap.factories import PreferencesFactory
from gap.models import Preferences


class PreferencesCRUDTest(TestCase):
    """Preferences test case."""

    def test_create(self):
        """Test create object."""
        obj = PreferencesFactory()
        self.assertIsInstance(obj, Preferences)
        self.assertTrue(Preferences.objects.filter(id=obj.id).exists())
        with self.assertRaises(IntegrityError):
            PreferencesFactory()

    def test_read(self):
        """Test read object."""
        obj = PreferencesFactory()
        fetched_obj = Preferences.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update(self):
        """Test update object."""
        obj = PreferencesFactory()
        new_geom = Polygon([
            (0.0, 0.0),
            (100.0, 0.0),
            (100.0, 100.0),
            (0.0, 100.0),
            (0.0, 0.0)
        ])
        obj.area_of_interest = new_geom
        obj.save()
        updated_provider = Preferences.objects.get(id=obj.id)
        self.assertEqual(updated_provider.area_of_interest, new_geom)

    def test_delete(self):
        """Test delete object."""
        provider = PreferencesFactory()
        provider_id = provider.id
        provider.delete()
        self.assertTrue(Preferences.objects.filter(id=provider_id).exists())
