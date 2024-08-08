# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase

from gap.factories import (
    CropFactory, FarmCategoryFactory, FarmRSVPStatusFactory,
    FarmFactory, VillageFactory
)
from gap.models import (
    Crop, FarmCategory, FarmRSVPStatus, Farm, Village
)


class CropCRUDTest(TestCase):
    """Crop test case."""

    Factory = CropFactory
    Model = Crop

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
        new_name = "Updated Name"
        obj.name = new_name
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.name, new_name)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmCategoryCRUDTest(TestCase):
    """FarmCategory test case."""

    Factory = FarmCategoryFactory
    Model = FarmCategory

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
        new_name = "Updated Name"
        obj.name = new_name
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.name, new_name)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmRSVPStatusCRUDTest(TestCase):
    """FarmRSVPStatus test case."""

    Factory = FarmRSVPStatusFactory
    Model = FarmRSVPStatus

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
        new_name = "Updated Name"
        obj.name = new_name
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.name, new_name)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class VillageCRUDTest(TestCase):
    """Village test case."""

    Factory = VillageFactory
    Model = Village

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
        new_name = "Updated Name"
        obj.name = new_name
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.name, new_name)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmCRUDTest(TestCase):
    """Farm test case."""

    Factory = FarmFactory
    Model = Farm

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
        new_id = "New ID"
        obj.unique_id = new_id
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.unique_id, new_id)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())
