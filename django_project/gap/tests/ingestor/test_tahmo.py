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
from gap.models.ingestor import IngestorSession, IngestorSessionStatus
from gap.models.measurement import Attribute
from gap.models.station import Station, Country


class TahmoIngestorTest(TestCase):
    """Tahmo ingestor test case."""

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
        """Test create provider object."""
        session = IngestorSession.objects.create()
        session.run()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_correct(self):
        """Test create provider object."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data',
            'TAHMO test stations.zip'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read())
        )
        session.run()
        session.delete()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        for station in Station.objects.all():
            self.assertEqual(station.country.name, 'Kenya')
            self.assertEqual(station.country.iso_a3, 'KEN')

        measurements = Station.objects.get(
            code='TA00056'
        ).measurement_set.all()
        for attribute in Attribute.objects.all():
            self.assertEqual(
                measurements.filter(attribute=attribute).count(), 23
            )
