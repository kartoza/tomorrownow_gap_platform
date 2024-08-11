# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Ingestor.
"""
import os

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.ingestor.exceptions import FileNotFoundException
from gap.ingestor.farm import Keys, Farm
from gap.models.ingestor import (
    IngestorSession, IngestorSessionStatus, IngestorType
)


class FarmIngestorTest(TestCase):
    """Farm ingestor test case."""

    fixtures = []

    def test_no_file(self):
        """Test no file ingestor."""
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.FARM
        )
        session.run()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_column(self):
        """Test when ingestor error column."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'ErrorColumn.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, f"Row 3 does not have '{Keys.GEOMETRY}'"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_error_coordinate(self):
        """Test when ingestor error coordinate is error."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'ErrorCoordinate.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM
        )
        session.run()
        session.delete()
        self.assertEqual(
            session.notes, "Row 3 : Invalid dms format"
        )
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_working(self):
        """Test when ingestor working fine."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data', 'farms', 'Correct.xlsx'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.FARM
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
        farms = Farm.objects.all()
        self.assertEqual(farms.count(), 3)
        self.assertEqual(farms[0].unique_id, '1001')
        self.assertEqual(farms[0].phone_number, '123-456-7890')
        self.assertEqual(farms[0].rsvp_status.name, 'Accepted')
        self.assertEqual(farms[0].category.name, 'Treatment')
        self.assertEqual(farms[0].village.name, 'Village A')
        self.assertEqual(farms[0].crop.name, 'Maize')
        self.assertEqual(farms[0].geometry.y, -0.2991111111111111)
        self.assertEqual(farms[0].geometry.x, 35.88930555555555)

        self.assertEqual(farms[1].unique_id, '1002')
        self.assertEqual(farms[1].phone_number, '123-456-7890')
        self.assertEqual(farms[1].rsvp_status.name, 'Declined')
        self.assertEqual(farms[1].category.name, 'Treatment')
        self.assertEqual(farms[1].village.name, 'Village B')
        self.assertEqual(farms[1].crop.name, 'Common Beans')
        self.assertEqual(farms[1].geometry.y, -0.29894444444444446)
        self.assertEqual(farms[1].geometry.x, 35.890972222222224)

        self.assertEqual(farms[2].unique_id, '1003')
        self.assertEqual(farms[2].phone_number, '0123456')
        self.assertEqual(farms[2].rsvp_status.name, 'Declined')
        self.assertEqual(farms[2].category.name, 'Control')
        self.assertEqual(farms[2].village.name, 'Village C')
        self.assertEqual(farms[2].crop.name, 'Wheat')
        self.assertEqual(farms[2].geometry.y, -0.2940277777777778)
        self.assertEqual(farms[2].geometry.x, 35.885)
