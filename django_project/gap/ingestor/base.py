# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Ingestor.
"""

from core.models import BackgroundTask
from gap.models import IngestorSession, IngestorSessionStatus


class BaseIngestor:
    """Ingestor Base class."""

    def __init__(self, session: IngestorSession):
        """Initialize ingestor."""
        self.session = session

    def is_cancelled(self):
        """Check if session is cancelled by user.

        This method will refetch the session object from DB.
        :return: True if session is gracefully cancelled.
        :rtype: bool
        """
        self.session.refresh_from_db()
        return self.session.is_cancelled


def ingestor_revoked_handler(bg_task: BackgroundTask):
    """Event handler when ingestor task is cancelled by celery.

    :param bg_task: background task
    :type bg_task: BackgroundTask
    """
    # retrieve ingestor session
    session = IngestorSession.objects.filter(
        id=int(bg_task.context_id)
    ).first()
    if session is None:
        return

    # update status as cancelled
    session.status = IngestorSessionStatus.CANCELLED
    session.save(update_fields=['status'])
