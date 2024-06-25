# ground_observations/factories.py
import factory
from factory.django import DjangoModelFactory
from gap.models import Provider, Attribute, Country, Station, Measurement
from django.contrib.gis.geos import Point, MultiPolygon

# class ProviderFactory(DjangoModelFactory):
#     class Meta:
#         model = Provider

#     provider = factory.LazyFunction(uuid.uuid4)
#     name = factory.Faker('company')
#     description = factory.Faker('text')

# class AttributeFactory(DjangoModelFactory):
#     class Meta:
#         model = Attribute

#     attribute = factory.LazyFunction(uuid.uuid4)
#     name = factory.Faker('word')
#     description = factory.Faker('text')

# class CountryFactory(DjangoModelFactory):
#     class Meta:
#         model = Country

#     name = factory.Faker('country')
#     iso_a3 = factory.Faker('country_code')
#     geometry = MultiPolygon([factory.LazyFunction(lambda: MultiPolygon(Point(0, 0)))])
#     description = factory.Faker('text')

# class StationFactory(DjangoModelFactory):
#     class Meta:
#         model = Station

#     station = factory.LazyFunction(uuid.uuid4)
#     name = factory.Faker('word')
#     country = factory.SubFactory(CountryFactory)
#     geometry = factory.LazyFunction(lambda: Point(0, 0))
#     provider = factory.SubFactory(ProviderFactory)
#     description = factory.Faker('text')

# class MeasurementFactory(DjangoModelFactory):
#     class Meta:
#         model = Measurement

#     station = factory.SubFactory(StationFactory)
#     attribute = factory.SubFactory(AttributeFactory)
#     date = factory.Faker('date')
#     value = factory.Faker('random_float')
