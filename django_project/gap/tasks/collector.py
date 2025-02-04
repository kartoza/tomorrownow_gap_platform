# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor Tasks.
"""

from celery.utils.log import get_task_logger

from django.core.mail import send_mail
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone

from core.celery import app
from core.models import BackgroundTask, TaskStatus
from gap.models import (
    Dataset,
    DatasetStore,
    DataSourceFile,
    IngestorType,
    IngestorSession,
    IngestorSessionStatus,
    CollectorSession
)
from gap.tasks.ingestor import (
    run_ingestor_session
)
from gap.utils.ingestor_config import get_ingestor_config_from_preferences

logger = get_task_logger(__name__)


@app.task(name='collector_session')
def run_collector_session(_id: int):
    """Run collector."""
    try:
        session = CollectorSession.objects.get(id=_id)
        session.run()
    except CollectorSession.DoesNotExist:
        logger.error(f"Collector Session {_id} does not exist")
        notify_collector_failure.delay(_id, "Collector session not found")
    except Exception as e:
        logger.error(f"Error in Collector Session {_id}: {str(e)}")
        notify_collector_failure.delay(_id, str(e))


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
        additional_conf = {}
        config = get_ingestor_config_from_preferences(dataset.provider)

        use_latest_datasource = config.get('use_latest_datasource', True)
        if use_latest_datasource:
            # find latest DataSourceFile
            data_source = DataSourceFile.objects.filter(
                dataset=dataset,
                format=DatasetStore.ZARR,
                is_latest=True
            ).last()
            if data_source:
                additional_conf = {
                    'datasourcefile_id': data_source.id,
                    'datasourcefile_exists': True
                }
        additional_conf.update(config)

        # create session and trigger the task
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


@app.task(name="notify_collector_failure")
def notify_collector_failure(session_id: int, exception: str):
    """
    Celery task to notify admins if a collector session fails.

    :param session_id: ID of the CollectorSession
    :param exception: Exception message describing the failure
    """
    # Retrieve the collector session
    try:
        session = CollectorSession.objects.get(id=session_id)
        session.status = IngestorSessionStatus.FAILED  # Ensure correct status
        session.save()
    except CollectorSession.DoesNotExist:
        logger.warning(f"CollectorSession {session_id} not found.")
        return

    background_task = BackgroundTask.objects.filter(
        task_name="notify_collector_failure",  # Updated for collectors
        context_id=str(session_id)
    ).first()

    if background_task:
        background_task.status = TaskStatus.STOPPED
        background_task.errors = exception
        background_task.last_update = timezone.now()
        background_task.save(update_fields=["status", "errors", "last_update"])
    else:
        logger.warning(
            f"No BackgroundTask found for collector session {session_id}"
        )

    # Log failure (If needed, adjust this for collectors)
    logger.error(f"CollectorSession {session_id} failed: {exception}")

    # Send an email notification to admins
    User = get_user_model()
    admin_emails = list(
        User.objects.filter(is_superuser=True).values_list('email', flat=True)
    )

    if admin_emails:
        send_mail(
            subject="Collector Failure Alert",
            message=(
                f"Collector Session {session_id} has failed.\n\n"
                f"Error: {exception}\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=admin_emails,
            fail_silently=False,
        )
        logger.info(f"Sent collector failure email to {admin_emails}")
    else:
        logger.warning("No admin email found in settings.ADMINS")

    return (
        f"Logged collector {session_id} failed. Admins notified."
    )
