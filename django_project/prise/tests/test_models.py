# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Prise Message Models.
"""

from django.test import TestCase
from django.utils import timezone

from message.factories import MessageTemplateFactory
from prise.factories import (
    PriseMessageFactory, PestFactory, PrisePestFactory,
    PriseDataFactory, PriseDataByPestFactory
)
from prise.models import (
    PriseMessage, PrisePest, PriseData, PriseDataByPest
)


class PriseMessageCRUDTest(TestCase):
    """PriseMessage test case."""

    Factory = PriseMessageFactory
    Model = PriseMessage

    def test_create_object(self):
        """Test create object."""
        obj = self.Factory()
        obj.messages.add(
            *[MessageTemplateFactory(), MessageTemplateFactory()]
        )
        self.assertIsInstance(obj, self.Model)
        self.assertTrue(self.Model.objects.filter(id=obj.id).exists())
        self.assertTrue(
            self.Model.objects.filter(id=obj.id).first().messages.count(), 2
        )

    def test_read_object(self):
        """Test read object."""
        obj = self.Factory()
        fetched_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_object(self):
        """Test update object."""
        obj = self.Factory()
        pest = PestFactory()
        obj.pest = pest
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.pest, pest)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class PrisePestCRUDTest(TestCase):
    """PrisePest test case."""

    Factory = PrisePestFactory
    Model = PrisePest

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
        new_variable_name = 'new name'
        obj.variable_name = new_variable_name
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.variable_name, new_variable_name)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class PriseDataCRUDTest(TestCase):
    """PriseData test case."""

    Factory = PriseDataFactory
    Model = PriseData

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
        new_ingested_at = timezone.now()
        obj.ingested_at = new_ingested_at
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.ingested_at, new_ingested_at)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class PriseDataByPestCRUDTest(TestCase):
    """PriseData test case."""

    Factory = PriseDataByPestFactory
    Model = PriseDataByPest

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
        new_value = 10.1
        obj.value = new_value
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.value, new_value)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())
