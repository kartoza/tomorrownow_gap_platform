# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Ingestor.
"""
import os

from django.contrib.gis.gdal import DataSource
from django.contrib.gis.geos import GEOSGeometry
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.ingestor.exceptions import FileNotFoundException
from gap.ingestor.grid import Keys
from gap.models.grid import Grid
from gap.models.ingestor import (
    IngestorType, IngestorSession, IngestorSessionStatus
)
from gap.models.station import Country


class FarmIngestorTest(TestCase):
    """Farm ingestor test case."""

    fixtures = []

    def setUp(self):
        """Init test case."""
        shp_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'Kenya.geojson'
        )
        data_source = DataSource(shp_path)
        layer = data_source[0]
        for feature in layer:
            geometry = GEOSGeometry(feature.geom.wkt, srid=4326)
            Country.objects.create(
                name=feature['name'],
                iso_a3=feature['iso_a3'],
                geometry=geometry
            )

    def test_no_file(self):
        """Test no file ingestor."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.FARM
        )
        session.run()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_file(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'excel.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, "File should be csv"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_column(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'error_incorrect_header.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes,
            f"Row 2 does not have '{Keys.UNIQUE_ID}'"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_wkt(self):
        """Test when ingestor error wkt."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'error_wkt.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes,
            f"Row 2 : wkt is not correct"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_correct(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'grid', 'correct.csv'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.GRID
        )
        session.run()
        session.delete()
        self.assertEqual(session.notes, '3/3')
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)

        grids = Grid.objects.all()
        self.assertEqual(grids.count(), 3)

        for grid in grids:
            self.assertEqual(grid.country.name, 'Kenya')

        grid = grids[0]
        self.assertEqual(grid.unique_id, '0001')
        self.assertEqual(
            [grid.geometry.centroid.y, grid.geometry.centroid.x],
            [0.012069999999999999, 35.554135]
        )
        grid = grids[1]
        self.assertEqual(grid.unique_id, '0002')
        self.assertEqual(
            [grid.geometry.centroid.y, grid.geometry.centroid.x],
            [0.012069999999999999, 35.590489999999996]
        )
        grid = grids[2]
        self.assertEqual(grid.unique_id, '0003')
        self.assertEqual(
            [grid.geometry.centroid.y, grid.geometry.centroid.x],
            [0.047935, 35.59049]
        )
