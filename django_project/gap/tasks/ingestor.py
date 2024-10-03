# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor Tasks.
"""

from celery.utils.log import get_task_logger

from core.celery import app
from gap.models.ingestor import IngestorSession, IngestorType

logger = get_task_logger(__name__)


@app.task(name='ingestor_session')
def run_ingestor_session(_id: int):
    """Run ingestor."""
    try:
        session = IngestorSession.objects.get(id=_id)
        session.run()
    except IngestorSession.DoesNotExist:
        logger.error('Ingestor Session {} does not exists'.format(_id))


@app.task(name='run_daily_ingestor')
def run_daily_ingestor():
    """Run Ingestor for arable."""
    for ingestor_type in [
        IngestorType.ARABLE, IngestorType.TAHMO_API,
        IngestorType.WIND_BORNE_SYSTEMS_API
    ]:
        session = IngestorSession.objects.filter(
            ingestor_type=ingestor_type
        ).first()
        if not session:
            # When created, it is autorun
            IngestorSession.objects.create(
                ingestor_type=ingestor_type
            )
        else:
            # When not created, it is run manually
            session.run()
