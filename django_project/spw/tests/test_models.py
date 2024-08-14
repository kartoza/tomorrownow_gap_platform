# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase

from spw.factories import (
    SPWOutputFactory
)
from spw.models import (
    SPWOutput
)


class SPWOutputCRUDTest(TestCase):
    """SPWOutput test case."""

    def test_create_obj(self):
        """Test create object."""
        obj = SPWOutputFactory()
        self.assertIsInstance(obj, SPWOutput)
        self.assertTrue(SPWOutput.objects.filter(id=obj.id).exists())

    def test_read_obj(self):
        """Test read object."""
        obj = SPWOutputFactory()
        fetched_obj = SPWOutput.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_obj(self):
        """Test update object."""
        obj = SPWOutputFactory()
        new_tier = "Tier 2"
        obj.tier = new_tier
        obj.save()
        updated_obj = SPWOutput.objects.get(id=obj.id)
        self.assertEqual(updated_obj.tier, new_tier)

    def test_delete_obj(self):
        """Test delete object."""
        obj = SPWOutputFactory()
        obj_id = obj.id
        obj.delete()
        self.assertFalse(SPWOutput.objects.filter(id=obj_id).exists())
