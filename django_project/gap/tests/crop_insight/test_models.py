# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from datetime import date

from django.test import TestCase

from gap.factories import (
    CropFactory, PestFactory, FarmShortTermForecastFactory,
    FarmProbabilisticWeatherForcastTableFactory,
    FarmSuitablePlantingWindowSignalFactory,
    FarmPlantingWindowTableFactory, FarmPestManagementFactory,
    FarmCropVarietyFactory
)
from gap.models import (
    Crop, Pest, FarmShortTermForecast, FarmProbabilisticWeatherForcastTable,
    FarmSuitablePlantingWindowSignal, FarmPlantingWindowTable,
    FarmPestManagement, FarmCropVariety
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


class PestCRUDTest(TestCase):
    """Pest test case."""

    Factory = PestFactory
    Model = Pest

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


class FarmShortTermForecastCRUDTest(TestCase):
    """FarmShortTermForecast test case."""

    Factory = FarmShortTermForecastFactory
    Model = FarmShortTermForecast

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
        new_value = 10
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


class FarmProbabilisticWeatherForcastTableCRUDTest(TestCase):
    """FarmProbabilisticWeatherForcastTable test case."""

    Factory = FarmProbabilisticWeatherForcastTableFactory
    Model = FarmProbabilisticWeatherForcastTable

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
        new_forecast_period = 'Daily'
        obj.forecast_period = new_forecast_period
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.forecast_period, new_forecast_period)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmSuitablePlantingWindowSignalCRUDTest(TestCase):
    """FarmSuitablePlantingWindowSignal test case."""

    Factory = FarmSuitablePlantingWindowSignalFactory
    Model = FarmSuitablePlantingWindowSignal

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
        new_signal = 'Signal 1'
        obj.signal = new_signal
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.signal, new_signal)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmPlantingWindowTableCRUDTest(TestCase):
    """FarmPlantingWindowTable test case."""

    Factory = FarmPlantingWindowTableFactory
    Model = FarmPlantingWindowTable

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
        new_date = date.today()
        obj.recommended_date = new_date
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(updated_obj.recommended_date, new_date)

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmPestManagementCRUDTest(TestCase):
    """FarmPestManagement test case."""

    Factory = FarmPestManagementFactory
    Model = FarmPestManagement

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
        spray_recommendation = 'Spray'
        obj.spray_recommendation = spray_recommendation
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(
            updated_obj.spray_recommendation, spray_recommendation
        )

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())


class FarmCropVarietyTest(TestCase):
    """FarmCropVariety test case."""

    Factory = FarmCropVarietyFactory
    Model = FarmCropVariety

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
        recommended_crop = CropFactory.create()
        obj.recommended_crop = recommended_crop
        obj.save()
        updated_obj = self.Model.objects.get(id=obj.id)
        self.assertEqual(
            updated_obj.recommended_crop.id, recommended_crop.id
        )

    def test_delete_object(self):
        """Test delete object."""
        obj = self.Factory()
        _id = obj.id
        obj.delete()
        self.assertFalse(self.Model.objects.filter(id=_id).exists())
