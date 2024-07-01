# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tasks.
"""

from celery import shared_task
from celery.utils.log import get_task_logger

from gap.models.ingestor import IngestorSession

logger = get_task_logger(__name__)


@shared_task(bind=True, queue='update')
def run_ingestor_session(self, _id: int):
    """Run ingestor."""
    try:
        session = IngestorSession.objects.get(id=_id)
        session.run()
    except IngestorSession.DoesNotExist:
        logger.debug('Ingestor Session {} does not exists'.format(_id))
