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
from gap.tasks.ingestor import run_ingestor_session

logger = get_task_logger(__name__)


@app.task(name='collector_session')
def run_collector_session(_id: int):
    """Run collector."""
    try:
        session = CollectorSession.objects.get(id=_id)
        session.run()
    except CollectorSession.DoesNotExist:
        logger.error('Collector Session {} does not exists'.format(_id))


@app.task(name='cbam_collector_session')
def run_cbam_collector_session():
    """Run Collector for CBAM Dataset."""
    session = CollectorSession.objects.create(
        ingestor_type=IngestorType.CBAM
    )
    session.run()
    if session.dataset_files.count() > 0:
        # create ingestor session to convert into zarr
        IngestorSession.objects.create(
            ingestor_type=IngestorType.CBAM,
            collector=session
        )


def _do_run_zarr_collector(
        dataset: Dataset, collector_session: CollectorSession,
        ingestor_type):
    """Run collector for zarr file.

    :param dataset: dataset
    :type dataset: Dataset
    :param collector_session: collector session to be run
    :type collector_session: CollectorSession
    :param ingestor_type: ingestor type
    :type ingestor_type: IngestorType
    """
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
            ingestor_type=ingestor_type,
            trigger_task=False,
            additional_config=additional_conf
        )
        session.collectors.add(collector_session)
        run_ingestor_session.delay(session.id)


@app.task(name='salient_collector_session')
def run_salient_collector_session():
    """Run Collector for Salient Dataset."""
    dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
    collector_session = CollectorSession.objects.create(
        ingestor_type=IngestorType.SALIENT
    )
    _do_run_zarr_collector(dataset, collector_session, IngestorType.SALIENT)


@app.task(name='tio_collector_session')
def run_tio_collector_session():
    """Run Collector for Tomorrow.io Dataset."""
    dataset = Dataset.objects.get(
        name='Tomorrow.io Short-term Forecast',
        store_type=DatasetStore.ZARR
    )
    # create the collector object
    collector_session = CollectorSession.objects.create(
        ingestor_type=IngestorType.TIO_FORECAST_COLLECTOR
    )
    _do_run_zarr_collector(dataset, collector_session, IngestorType.TOMORROWIO)
