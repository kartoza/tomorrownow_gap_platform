# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Ingestor.
"""

from typing import Union

from core.models import BackgroundTask
from gap.models import (
    CollectorSession,
    IngestorSession,
    IngestorSessionStatus
)


class BaseIngestor:
    """Collector/Ingestor Base class."""

    def __init__(
        self,
        session: Union[CollectorSession, IngestorSession],
        working_dir: str
    ):
        """Initialize ingestor/collector."""
        self.session = session
        self.working_dir = working_dir

    def is_cancelled(self):
        """Check if session is cancelled by user.

        This method will refetch the session object from DB.
        :return: True if session is gracefully cancelled.
        :rtype: bool
        """
        self.session.refresh_from_db()
        return self.session.is_cancelled

    def get_config(self, name: str, default_value = None):
        """Get config from session.

        :param name: config name
        :type name: str
        :param default_value: default value if config does not exist,
            defaults to None
        :type default_value: any, optional
        :return: config value or default_value
        :rtype: any
        """
        if self.session.additional_config is None:
            return default_value
        return self.session.additional_config.get(name, default_value)


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
