# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS tasks.
"""

from celery import shared_task
import logging
from datetime import datetime, timedelta
from dcas.queries import DataQuery
from dcas.models import DCASErrorLog, DCASRequest, DCASErrorType
from dcas.outputs import DCASPipelineOutput
from gap.models.farm import Farm

logger = logging.getLogger(__name__)


@shared_task(name='log_farms_without_messages')
def log_farms_without_messages(request_date=None):
    """
    Celery task to log farms without messages.

    :param request_date: Date for the pipeline output
    :type request_date: datetime.date
    """
    if request_date is None:
        request_date = (datetime.utcnow() - timedelta(days=1)).date()
    logger.info('Checking for farms without messages...')

    try:
        # Get the most recent DCAS request
        dcas_request = DCASRequest.objects.filter(
            request_date=request_date
        ).latest('created_at')

        # Initialize pipeline output to get the directory path
        dcas_output = DCASPipelineOutput(request_date)
        parquet_path = dcas_output._get_directory_path(
            dcas_output.DCAS_OUTPUT_DIR + '/*.parquet'
        )

        # Query farms without messages
        df = DataQuery.get_farms_without_messages(parquet_path)

        if df.empty:
            logger.info('No farms found with missing messages.')
            return

        # Log missing messages in the database
        error_logs = []
        for _, row in df.iterrows():
            try:
                farm = Farm.objects.get(id=row['farm_id'])
            except Farm.DoesNotExist:
                logger.warning(
                    f"Farm ID {row['farm_id']} not found, skipping.")
                continue

            error_logs.append(DCASErrorLog(
                request=dcas_request,
                farm_id=farm,
                error_type=DCASErrorType.MISSING_MESSAGES,
                error_message=(
                    f"Farm {row['farm_id']} (Crop {row['crop_id']}) "
                    f"has no advisory messages."
                )
            ))

        # Bulk insert to improve performance
        if error_logs:
            DCASErrorLog.objects.bulk_create(error_logs)
            logger.info(
                f"Logged {len(error_logs)} farms with missing messages.")

    except DCASRequest.DoesNotExist:
        logger.error(
            f"No DCASRequest found for request_date {request_date}.")
    except Exception as e:
        logger.error(f"Error processing missing messages: {str(e)}")
