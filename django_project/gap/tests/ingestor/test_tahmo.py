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
from gap.models.station import Station, Country


class TahmoIngestorTest(TestCase):
    """Tahmo ingestor test case."""

    fixtures = [
        '2.provider.json',
        '3.observation_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

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
        self.assertEqual(
            session.ingestorsessionprogress_set.filter(
                session=session,
                status=IngestorSessionStatus.FAILED
            ).count(), 0
        )
        session.delete()
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        self.assertEqual(Station.objects.count(), 2)
        for station in Station.objects.all():
            self.assertEqual(station.country.name, 'Kenya')
            self.assertEqual(station.country.iso_a3, 'KEN')
        measurements = Station.objects.get(
            code='TA00001'
        ).measurement_set.all()
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_min'
                ).values_list('value', flat=True)
            ),
            [14.859377, 17.1, 17.2]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_max'
                ).values_list('value', flat=True)
            ),
            [24.111788, 30.9, 24]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='te_avg'
                ).values_list('value', flat=True)
            ),
            [19.1567865, 21.15, 18.47916667]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='rh_min'
                ).values_list('value', flat=True)
            ),
            [0.651, 0.429, 0.682]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='rh_max'
                ).values_list('value', flat=True)
            ),
            [0.995, 1, 1]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='ra_total'
                ).values_list('value', flat=True)
            ),
            [2070.156154, 5973, 1836]
        )
        self.assertEqual(
            list(
                measurements.filter(
                    dataset_attribute__source='pr_total'
                ).values_list('value', flat=True)
            ),
            [1.885, 1.7, 15.521]
        )
