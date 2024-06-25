from django.test import TestCase
from gap.models import (
    Provider, Attribute, Country, Station, Measurement
)
from gap.factories import (
    ProviderFactory,
    AttributeFactory,
    CountryFactory,
    StationFactory,
    MeasurementFactory
)

class ProviderCRUDTest(TestCase):
    """Provider test case"""

    def test_create_provider(self):
        provider = ProviderFactory()
        self.assertIsInstance(provider, Provider)
        self.assertTrue(Provider.objects.filter(id=provider.id).exists())

    def test_read_provider(self):
        provider = ProviderFactory()
        fetched_provider = Provider.objects.get(id=provider.id)
        self.assertEqual(provider, fetched_provider)

    def test_update_provider(self):
        provider = ProviderFactory()
        new_name = "Updated Provider Name"
        provider.name = new_name
        provider.save()
        updated_provider = Provider.objects.get(id=provider.id)
        self.assertEqual(updated_provider.name, new_name)

    def test_delete_provider(self):
        provider = ProviderFactory()
        provider_id = provider.id
        provider.delete()
        self.assertFalse(Provider.objects.filter(id=provider_id).exists())


class AttributeCRUDTest(TestCase):
    """Attribute test case"""

    def test_create_attribute(self):
        attribute = AttributeFactory()
        self.assertIsInstance(attribute, Attribute)
        self.assertTrue(Attribute.objects.filter(id=attribute.id).exists())

    def test_read_attribute(self):
        attribute = AttributeFactory()
        fetched_attribute = Attribute.objects.get(id=attribute.id)
        self.assertEqual(attribute, fetched_attribute)

    def test_update_attribute(self):
        attribute = AttributeFactory()
        new_name = "Updated Attribute Name"
        attribute.name = new_name
        attribute.save()
        updated_attribute = Attribute.objects.get(id=attribute.id)
        self.assertEqual(updated_attribute.name, new_name)

    def test_delete_attribute(self):
        attribute = AttributeFactory()
        attribute_id = attribute.id
        attribute.delete()
        self.assertFalse(Attribute.objects.filter(id=attribute_id).exists())


class CountryCRUDTest(TestCase):
    """Country test case"""

    def test_create_country(self):
        country = CountryFactory()
        self.assertIsInstance(country, Country)
        self.assertTrue(Country.objects.filter(id=country.id).exists())

    def test_read_country(self):
        country = CountryFactory()
        fetched_country = Country.objects.get(id=country.id)
        self.assertEqual(country, fetched_country)

    def test_update_country(self):
        country = CountryFactory()
        new_name = "Updated Country Name"
        country.name = new_name
        country.save()
        updated_country = Country.objects.get(id=country.id)
        self.assertEqual(updated_country.name, new_name)

    def test_delete_country(self):
        country = CountryFactory()
        country_id = country.id
        country.delete()
        self.assertFalse(Country.objects.filter(id=country_id).exists())


class StationCRUDTest(TestCase):
    """Station test case"""

    def test_create_station(self):
        station = StationFactory()
        self.assertIsInstance(station, Station)
        self.assertTrue(Station.objects.filter(id=station.id).exists())

    def test_read_station(self):
        station = StationFactory()
        fetched_station = Station.objects.get(id=station.id)
        self.assertEqual(station, fetched_station)

    def test_update_station(self):
        station = StationFactory()
        new_name = "Updated Station Name"
        station.name = new_name
        station.save()
        updated_station = Station.objects.get(id=station.id)
        self.assertEqual(updated_station.name, new_name)

    def test_delete_station(self):
        station = StationFactory()
        station_id = station.id
        station.delete()
        self.assertFalse(Station.objects.filter(id=station_id).exists())


class MeasurementCRUDTest(TestCase):
    """Measurement test case"""

    def test_create_measurement(self):
        measurement = MeasurementFactory()
        self.assertIsInstance(measurement, Measurement)
        self.assertTrue(Measurement.objects.filter(id=measurement.id).exists())

    def test_read_measurement(self):
        measurement = MeasurementFactory()
        fetched_measurement = Measurement.objects.get(id=measurement.id)
        self.assertEqual(measurement, fetched_measurement)

    def test_update_measurement(self):
        measurement = MeasurementFactory()
        new_value = 42.0
        measurement.value = new_value
        measurement.save()
        updated_measurement = Measurement.objects.get(id=measurement.id)
        self.assertEqual(updated_measurement.value, new_value)

    def test_delete_measurement(self):
        measurement = MeasurementFactory()
        measurement_id = measurement.id
        measurement.delete()
        self.assertFalse(Measurement.objects.filter(id=measurement_id).exists())
