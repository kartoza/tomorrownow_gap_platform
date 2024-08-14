# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP LookupModels.
"""

from django.test import TestCase

from gap.factories import (
    RainfallClassificationFactory
)
from gap.models import (
    RainfallClassification
)


class RainfallClassificationCRUDTest(TestCase):
    """RainfallClassification test case."""

    def test_create_obj(self):
        """Test create obj object."""
        obj = RainfallClassificationFactory()
        self.assertIsInstance(obj, RainfallClassification)
        self.assertTrue(
            RainfallClassification.objects.filter(id=obj.id).exists()
        )

    def test_read_obj(self):
        """Test read object."""
        obj = RainfallClassificationFactory()
        fetched_obj = RainfallClassification.objects.get(id=obj.id)
        self.assertEqual(obj, fetched_obj)

    def test_update_obj(self):
        """Test update object."""
        obj = RainfallClassificationFactory()
        new_name = "Test"
        obj.name = new_name
        obj.save()
        updated_obj = RainfallClassification.objects.get(id=obj.id)
        self.assertEqual(updated_obj.name, new_name)

    def test_delete_obj(self):
        """Test delete object."""
        obj = RainfallClassificationFactory()
        obj_id = obj.id
        obj.delete()
        self.assertFalse(
            RainfallClassification.objects.filter(id=obj_id).exists()
        )


class RainfallClassificationTest(TestCase):
    """RainfallClassification test case."""

    fixtures = [
        '9.rainfall_classification.json'
    ]

    def test_classification(self):
        """Test classification value."""
        self.assertEqual(RainfallClassification.classify(0).pk, 1)
        self.assertEqual(RainfallClassification.classify(0.5).pk, 1)
        self.assertEqual(RainfallClassification.classify(3).pk, 2)
        self.assertEqual(RainfallClassification.classify(4).pk, 3)
        self.assertEqual(RainfallClassification.classify(11).pk, 4)
        self.assertEqual(RainfallClassification.classify(21).pk, 5)
