# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Geometry test.
"""

from django.contrib.gis.geos import Polygon
from django.test import TestCase

from gap.utils.geometry import split_polygon_to_bbox


class GeometryTest(TestCase):
    """Geometry test case."""

    def test_error(self):
        """Test create when error."""
        geom = Polygon(
            [
                (0.0, 0.0),
                (100.0, 0.0),
                (100.0, 100.0),
                (0.0, 100.0),
                (0.0, 0.0)
            ]
        )
        with self.assertRaises(ValueError):
            split_polygon_to_bbox(geom, 1)

    def test_run_3857(self):
        """Test create when run."""
        geom = Polygon(
            [
                (0.0, 0.0),
                (95.0, 0.0),
                (95.0, 95.0),
                (0.0, 95.0),
                (0.0, 0.0)
            ], srid=3857
        )
        ouput = split_polygon_to_bbox(geom, 10)
        self.assertEqual(len(ouput), 100)
        self.assertEqual(
            [int(coord) for coord in ouput[len(ouput) - 1].boundary.extent],
            [90, 90, 95, 95]
        )

    def test_run_3857_hexagon(self):
        """Test create when run."""
        geom = Polygon(
            [
                [0.0, 0.0],
                [50.0, 0.0],
                [100.0, 50.0],
                [100.0, 100.0],
                [50.0, 100.0],
                [0.0, 50.0],
                [0.0, 0.0]
            ], srid=3857
        )
        ouput = split_polygon_to_bbox(geom, 10)
        self.assertEqual(len(ouput), 80)

    def test_run_4326(self):
        """Test create when run."""
        geom = Polygon(
            [
                [0.0, 0.0],
                [0.00089831528412, 0.0],
                [0.00089831528412, 0.000898315284083],
                [0.0, 0.000898315284083], [0.0, 0.0]
            ], srid=4326
        )
        ouput = split_polygon_to_bbox(geom, 10)
        self.assertEqual(len(ouput), 100)
