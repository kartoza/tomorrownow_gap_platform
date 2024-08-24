# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor Tasks.
"""

from celery.utils.log import get_task_logger

from core.celery import app
from gap.models.ingestor import IngestorSession

logger = get_task_logger(__name__)


@app.task(name='ingestor_session')
def run_ingestor_session(_id: int):
    """Run ingestor."""
    try:
        session = IngestorSession.objects.get(id=_id)
        session.run()
    except IngestorSession.DoesNotExist:
        logger.debug('Ingestor Session {} does not exists'.format(_id))
