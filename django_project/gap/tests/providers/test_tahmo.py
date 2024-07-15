# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Reader.
"""

from django.test import TestCase
from datetime import datetime
from django.contrib.gis.geos import Point

from gap.providers import (
    TahmoDatasetReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    StationFactory,
    MeasurementFactory
)


class TestTahmoReader(TestCase):
    """Unit test for Tahmo NetCDFReader class."""

    def test_read_historical_data(self):
        """Test for reading historical data from Tahmo."""
        dataset = DatasetFactory.create(
            provider=ProviderFactory(name='Tahmo'))
        attribute = AttributeFactory.create(
            name='Surface air temperature',
            variable_name='surface_air_temperature')
        dataset_attr = DatasetAttributeFactory.create(
            dataset=dataset,
            attribute=attribute,
            source='surface_air_temperature'
        )
        dt = datetime(2019, 11, 1, 0, 0, 0)
        p = Point(x=26.97, y=-12.56, srid=4326)
        station = StationFactory.create(
            geometry=p,
            provider=dataset.provider
        )
        MeasurementFactory.create(
            station=station,
            dataset_attribute=dataset_attr,
            date_time=dt,
            value=100
        )
        reader = TahmoDatasetReader(
            dataset, [dataset_attr], p, dt, dt)
        reader.read_historical_data(dt, dt)
        data_value = reader.get_data_values()
        self.assertEqual(len(data_value.results), 1)
        self.assertEqual(
            data_value.results[0].values['surface_air_temperature'],
            100)
