# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor Tasks.
"""

from celery.utils.log import get_task_logger

from core.celery import app
from gap.models.dataset import (
    Dataset,
    DatasetStore,
    DataSourceFile
)
from gap.models.ingestor import (
    IngestorType,
    IngestorSession,
    CollectorSession
)

logger = get_task_logger(__name__)


@app.task(name='ingestor_session')
def run_ingestor_session(_id: int):
    """Run ingestor."""
    try:
        session = IngestorSession.objects.get(id=_id)
        session.run()
    except IngestorSession.DoesNotExist:
        logger.error('Ingestor Session {} does not exists'.format(_id))


@app.task(name='collector_session')
def run_collector_session(_id: int):
    """Run collector."""
    try:
        session = CollectorSession.objects.get(id=_id)
        session.run()
    except CollectorSession.DoesNotExist:
        logger.error('Collector Session {} does not exists'.format(_id))


@app.task(name='salient_collector_session')
def run_salient_collector_session():
    """Run Collector for Salient Dataset."""
    dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
    # create the collector object
    collector_session = CollectorSession.objects.create(
        ingestor_type=IngestorType.SALIENT
    )
    # run collector
    collector_session.run()

    # if success, create ingestor session
    collector_session.refresh_from_db()
    total_file = collector_session.dataset_files.count()
    if total_file > 0:
        # find latest DataSourceFile
        data_source = DataSourceFile.objects.filter(
            dataset=dataset,
            format=DatasetStore.ZARR,
            is_latest=True
        ).last()
        additional_conf = {}
        if data_source:
            additional_conf = {
                'datasourcefile_id': data_source.id,
                'datasourcefile_zarr_exists': True
            }
        session = IngestorSession.objects.create(
            ingestor_type=IngestorType.SALIENT,
            trigger_task=False,
            additional_config=additional_conf
        )
        session.collectors.add(collector_session)
        run_ingestor_session.delay(session.id)
