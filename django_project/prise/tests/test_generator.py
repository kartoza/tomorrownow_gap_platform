# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for generator functions.
"""

from django.test import TestCase

from prise.variables import PriseDataType, PriseMessageGroup


class PriseGeneratorTest(TestCase):
    """Unit tests for Prise generator."""

    def test_get_type_by_message_group(self):
        """Test get_type_by_message_group."""
        type = PriseDataType.get_type_by_message_group(
            PriseMessageGroup.TIME_TO_ACTION_1)
        self.assertEqual(type, PriseDataType.CLIMATOLOGY)
        type = PriseDataType.get_type_by_message_group(
            PriseMessageGroup.TIME_TO_ACTION_2)
        self.assertEqual(type, PriseDataType.NEAR_REAL_TIME)
        type = PriseDataType.get_type_by_message_group(
            PriseMessageGroup.START_SEASON)
        self.assertFalse(type)
