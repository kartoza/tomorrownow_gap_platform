# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS tasks.
"""

from celery import shared_task
import logging
from dcas.queries import DataQuery
from dcas.models import DCASErrorLog, DCASRequest, DCASErrorType
from gap.models.farm import Farm

logger = logging.getLogger(__name__)


@shared_task(name='log_farms_without_messages')
def log_farms_without_messages(parquet_path: str, request_id: int):
    """
    Celery task to log farms without messages.

    :param parquet_path: Path to the final farm crop Parquet file
    :type parquet_path: str
    :param request_id: ID of the related DCASRequest
    :type request_id: int
    """
    logger.info('Checking for farms without messages...')

    try:
        # Query farms without messages
        df = DataQuery.get_farms_without_messages(parquet_path)

        if df.empty:
            logger.info('No farms found with missing messages.')
            return

        # Retrieve the associated DCAS request
        try:
            dcas_request = DCASRequest.objects.get(id=request_id)
        except DCASRequest.DoesNotExist:
            logger.error(f'DCASRequest with ID {request_id} not found.')
            return

        # Log missing messages in the database
        error_logs = []
        for _, row in df.iterrows():
            try:
                farm = Farm.objects.get(id=row['farm_id'])
            except Farm.DoesNotExist:
                logger.warning(
                    f'Farm ID {row['farm_id']} not found, skipping.'
                )
                continue

            error_logs.append(DCASErrorLog(
                request=dcas_request,
                farm_id=farm,
                error_type=DCASErrorType.MISSING_MESSAGES,
                error_message=(
                    f'Farm {row['farm_id']} (Crop {row['crop_id']}) '
                    f'has no advisory messages.'
                )
            ))

        # Bulk insert to improve performance
        if error_logs:
            DCASErrorLog.objects.bulk_create(error_logs)
            logger.info(
                f'Logged {len(error_logs)} farms with missing messages.'
            )

    except Exception as e:
        logger.error(f'Error processing missing messages: {str(e)}')
