# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor Tasks.
"""

from celery.utils.log import get_task_logger

import uuid
from django.core.mail import send_mail
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone

from core.celery import app
from core.models import BackgroundTask, TaskStatus
from gap.models import Dataset, DataSourceFile, Preferences, DatasetStore
from gap.models.ingestor import (
    IngestorSession, IngestorType,
    IngestorSessionStatus
)
from gap.utils.parquet import (
    ParquetConverter,
    WindborneParquetConverter
)

logger = get_task_logger(__name__)


@app.task(name='ingestor_session')
def run_ingestor_session(_id: int):
    """Run ingestor."""
    try:
        session = IngestorSession.objects.get(id=_id)
        session.run()
    except IngestorSession.DoesNotExist:
        logger.error(f"Ingestor Session {_id} does not exist")
        notify_ingestor_failure.delay(_id, "Session not found")
    except Exception as e:
        logger.error(f"Error in Ingestor Session {_id}: {str(e)}")
        notify_ingestor_failure.delay(_id, str(e))


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


@app.task(name="notify_ingestor_failure")
def notify_ingestor_failure(session_id: int, exception: str):
    """
    Celery task to notify admins if an ingestor session fails.

    :param session_id: ID of the IngestorSession
    :param exception: Exception message describing the failure
    """
    # Retrieve the ingestor session
    try:
        session = IngestorSession.objects.get(id=session_id)
        session.status = IngestorSessionStatus.FAILED
        session.save()
    except IngestorSession.DoesNotExist:
        logger.warning(f"IngestorSession {session_id} not found.")
        return

    # Log failure in BackgroundTask
    background_task = BackgroundTask.objects.filter(
        task_name="notify_ingestor_failure",
        context_id=str(session_id)
    ).first()

    if background_task:
        background_task.status = TaskStatus.STOPPED
        background_task.errors = exception
        background_task.last_update = timezone.now()
        background_task.save(update_fields=["status", "errors", "last_update"])
    else:
        logger.warning(f"No BackgroundTask found for session {session_id}")

    # Send an email notification to admins
    # Get admin emails from the database
    User = get_user_model()
    admin_emails = list(
        User.objects.filter(
            is_superuser=True).values_list('email', flat=True)
    )
    if admin_emails:
        send_mail(
            subject="Ingestor Failure Alert",
            message=(
                f"Ingestor Session {session_id} has failed.\n\n"
                f"Error: {exception}\n\n"
                "Please check the logs for more details."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[admin_emails],
            fail_silently=False,
        )
        logger.info(f"Sent ingestor failure email to {admin_emails}")
    else:
        logger.warning("No admin email found in settings.ADMINS")

    return (
        f"Logged ingestor failure for session {session_id} and notified admins"
    )


def _get_data_source(dataset: Dataset, provider_conf: dict):
    # get latest one if exists
    data_source = DataSourceFile.objects.filter(
        dataset=dataset,
        format=DatasetStore.PARQUET,
        is_latest=True
    ).last()
    if data_source:
        return data_source

    data_source = DataSourceFile.objects.create(
        dataset=dataset,
        format=DatasetStore.PARQUET,
        name=provider_conf.get('parquet_name', str(uuid.uuid4())),
        start_date_time=timezone.now(),
        end_date_time=timezone.now(),
        created_on=timezone.now()
    )
    return data_source


@app.task(name='convert_dataset_to_parquet')
def convert_dataset_to_parquet(dataset_id: int):
    """Convert dataset EAV to parquet files."""
    dataset = Dataset.objects.get(id=dataset_id)
    if dataset.name not in [
        'Tahmo Ground Observational',
        'Arable Ground Observational',
        'Tahmo Disdrometer Observational',
        'WindBorne Balloons Observations'
    ]:
        raise ValueError(
            f'Invalid dataset {dataset.name} to be converted into parquet!'
        )

    converter_class = ParquetConverter
    if dataset.name == 'WindBorne Balloons Observations':
        converter_class = WindborneParquetConverter

    ingestor_conf = Preferences.load().ingestor_config
    provider_conf = ingestor_conf.get(dataset.provider.name, {})
    data_source = _get_data_source(dataset, provider_conf)

    converter = converter_class(dataset, data_source)
    converter.setup()
    converter.run()
