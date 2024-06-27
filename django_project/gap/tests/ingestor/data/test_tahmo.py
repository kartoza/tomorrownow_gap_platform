# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Ingestor.
"""

import os

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from gap.ingestor.exceptions import FileNotFoundException
from gap.models.ingestor import IngestorSession, IngestorSessionStatus


class TahmoIngestorTest(TestCase):
    """Tahmo ingestor test case."""

    def test_no_file(self):
        """Test create provider object."""
        session = IngestorSession.objects.create()
        self.assertEqual(session.notes, FileNotFoundException().message)
        self.assertEqual(session.status, IngestorSessionStatus.FAILED)

    def test_correct(self):
        """Test create provider object."""
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'TAHMO test stations.zip'
        )
        _file = open(filepath, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read())
        )
        self.assertEqual(session.notes, None)
        self.assertEqual(session.status, IngestorSessionStatus.SUCCESS)
        session.delete()
