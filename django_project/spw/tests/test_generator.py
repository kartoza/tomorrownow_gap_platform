# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""
from django.test import TestCase
from django.contrib.gis.geos import Point

from spw.generator import SPWOutput


class TestSPWOutput(TestCase):
    """Unit test for SPWOutput class."""

    def setUp(self):
        """Set the test class."""
        self.point = Point(y=1.0, x=1.0)
        self.input_data = {
            'temperature': [20.5],
            'pressure': [101.3],
            'humidity': 45,
            'metadata': 'some metadata'
        }
        self.expected_data = {
            'temperature': 20.5,
            'pressure': 101.3,
            'humidity': 45
        }

    def test_initialization(self):
        """Test initialization of SPWOutput class."""
        spw_output = SPWOutput(self.point, self.input_data)

        self.assertEqual(spw_output.point, self.point)
        self.assertEqual(
            spw_output.data.temperature, self.expected_data['temperature'])
        self.assertEqual(
            spw_output.data.pressure, self.expected_data['pressure'])
        self.assertEqual(
            spw_output.data.humidity, self.expected_data['humidity'])

    def test_input_data_without_metadata(self):
        """Test initialization of SPWOutput class without metadata."""
        input_data = {
            'temperature': [20.5],
            'pressure': [101.3],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'][0])
        self.assertEqual(spw_output.data.pressure, input_data['pressure'][0])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])

    def test_input_data_with_single_element_list(self):
        """Test initialization of SPWOutput class for single element."""
        input_data = {
            'temperature': [20.5],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'][0])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])

    def test_input_data_with_multiple_element_list(self):
        """Test initialization of SPWOutput class for list."""
        input_data = {
            'temperature': [20.5, 21.0],
            'humidity': 45
        }
        spw_output = SPWOutput(self.point, input_data)

        self.assertEqual(
            spw_output.data.temperature, input_data['temperature'])
        self.assertEqual(spw_output.data.humidity, input_data['humidity'])
