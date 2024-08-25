# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Ingestor.
"""

from gap.models import IngestorSession, IngestorSessionStatus


class BaseIngestor:
    """Ingestor Base class."""

    def __init__(self, session: IngestorSession):
        """Initialize ingestor."""
        self.session = session

    def is_cancelled(self):
        self.session.refresh_from_db()
        return self.session.is_cancelled
