# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DMS test.
"""

from django.test import TestCase

from gap.utils.dms import dms_string_to_point


class DMSTest(TestCase):
    """DMS test case."""

    def test_error(self):
        """Test create when error."""
        with self.assertRaises(ValueError):
            dms_string_to_point('This error')

    def test_run(self):
        """Test create when run."""
        point = dms_string_to_point(
            '''33°51'29.8"N 29°18'24.3"E'''
        )
        self.assertEqual(point.y, 33.85827777777778)
        self.assertEqual(point.x, 29.30675)

        point = dms_string_to_point(
            '''34°3'12.8"S 118°14'54.5"W'''
        )
        self.assertEqual(point.y, -34.053555555555555)
        self.assertEqual(point.x, -118.24847222222222)
