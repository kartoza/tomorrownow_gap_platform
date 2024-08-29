# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Base Ingestor.
"""

from django.test import TestCase

from core.factories import BackgroundTaskF
from gap.models.ingestor import IngestorSession, IngestorSessionStatus
from gap.ingestor.base import BaseIngestor, ingestor_revoked_handler


class BaseIngestorTest(TestCase):
    """Base ingestor test case."""

    def test_is_cancelled(self):
        """Test is_cancelled method."""
        session = IngestorSession.objects.create()
        ingestor = BaseIngestor(
            IngestorSession.objects.get(id=session.id), '/tmp')
        self.assertFalse(ingestor.is_cancelled())
        session.is_cancelled = True
        session.save()
        self.assertTrue(ingestor.is_cancelled())

    def test_ingestor_revoked_handler(self):
        """Test ingestor revoked handler."""
        session = IngestorSession.objects.create()
        bg_task = BackgroundTaskF.create(
            context_id=str(session.id)
        )
        ingestor_revoked_handler(bg_task)
        session.refresh_from_db()
        self.assertEqual(session.status, IngestorSessionStatus.CANCELLED)
