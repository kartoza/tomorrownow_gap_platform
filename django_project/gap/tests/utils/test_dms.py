# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DMS test.
"""

from django.test import TestCase

from gap.utils.dms import dms_string_to_lat_lon


class DMSTest(TestCase):
    """DMS test case."""

    def test_error(self):
        """Test create when error."""
        with self.assertRaises(ValueError):
            dms_string_to_lat_lon('This error')

    def test_run(self):
        """Test create when run."""
        latitude, longitude = dms_string_to_lat_lon(
            '''33°51'29.8"N 29°18'24.3"E'''
        )
        self.assertEqual(latitude, 33.8582778)
        self.assertEqual(longitude, 29.3067500)

        latitude, longitude = dms_string_to_lat_lon(
            '''34°3'12.8"S 118°14'54.5"W'''
        )
        self.assertEqual(latitude, -34.0535556)
        self.assertEqual(longitude, -118.2484722)
