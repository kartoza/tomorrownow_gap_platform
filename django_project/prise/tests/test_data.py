# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Message Models.
"""

from django.test import TestCase
from django.utils import timezone

from gap.exceptions import FarmWithUniqueIdDoesNotFound
from gap.factories import PestFactory, FarmFactory
from prise.exceptions import PestVariableNameNotRecognized
from prise.factories import PrisePestFactory
from prise.models.data import (
    PriseData, PriseDataRawInput, PriseDataByPestRawInput
)


class PriseDataTest(TestCase):
    """PriseData test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set SalientIngestorBaseTest."""
        self.pest_1 = PestFactory(name='pest 1')
        self.pest_2 = PestFactory(name='pest 2')
        PrisePestFactory(
            pest=self.pest_1, variable_name='pest_1'
        )
        PrisePestFactory(
            pest=self.pest_2, variable_name='pest_2'
        )
        self.farm = FarmFactory(unique_id='farm_1')

    def test_farm_unique_id_does_not_exist(self):
        """Test return farm with unique id does not exist."""
        with self.assertRaises(FarmWithUniqueIdDoesNotFound):
            PriseDataRawInput(
                farm_unique_id='farm_0', generated_at=timezone.now(),
                values=[]
            )

    def test_pest_variable_not_found(self):
        """Test return pest with variable does not exist."""
        with self.assertRaises(PestVariableNameNotRecognized):
            PriseDataRawInput(
                farm_unique_id='farm_1', generated_at=timezone.now(),
                values=[
                    PriseDataByPestRawInput('pest_0', 10)
                ]
            )

    def test_prise_ingest_data(self):
        """Test ingest data."""
        date_time = timezone.now()
        PriseData.insert_data(
            PriseDataRawInput(
                farm_unique_id='farm_1', generated_at=date_time,
                values=[
                    PriseDataByPestRawInput('pest_1', 10),
                    PriseDataByPestRawInput('pest_2', 20),
                ]
            )
        )
        data = PriseData.objects.get(farm=self.farm)
        self.assertEqual(data.farm, self.farm)
        self.assertEqual(data.generated_at, date_time)
        self.assertEqual(data.prisedatabypest_set.count(), 2)
        self.assertEqual(
            data.prisedatabypest_set.get(pest=self.pest_1).value, 10
        )
        self.assertEqual(
            data.prisedatabypest_set.get(pest=self.pest_2).value, 20
        )
