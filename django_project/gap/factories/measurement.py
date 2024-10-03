# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Measurement objects.
"""
import factory
from factory.django import DjangoModelFactory

from gap.factories.main import DatasetAttributeFactory
from gap.factories.station import StationFactory
from gap.models import Measurement


class MeasurementFactory(DjangoModelFactory):
    """Factory class for Measurement model."""

    class Meta:  # noqa
        model = Measurement

    station = factory.SubFactory(StationFactory)
    dataset_attribute = factory.SubFactory(DatasetAttributeFactory)
    date_time = factory.Faker('date_time')
    value = factory.Faker('pyfloat')
