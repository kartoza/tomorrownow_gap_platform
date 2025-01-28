# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Tasks
"""

import os
from celery import shared_task
import datetime
import logging
from django.core.files.storage import default_storage

from gap.models import FarmRegistryGroup, FarmRegistry, Preferences, Farm
from dcas.models import DCASErrorLog, DCASRequest, DCASErrorType
from dcas.pipeline import DCASDataPipeline
from dcas.queries import DataQuery
from dcas.outputs import DCASPipelineOutput


logger = logging.getLogger(__name__)


@shared_task(name="run_dcas")
def run_dcas():
    """Task to run dcas pipeline."""
    current_dt = datetime.datetime.now()
    dcas_config = Preferences.load().dcas_config

    if current_dt.weekday() != dcas_config.get('weekday'):
        logger.info(f'DCAS: skipping weekday {current_dt.weekday()}')
        return

    # load latest farm registry group
    farm_registry_group = FarmRegistryGroup.objects.filter(
        is_latest=True
    ).first()
    if farm_registry_group is None:
        logger.warning('DCAS: No latest farm registry group')
        return

    # check total count
    total_count = FarmRegistry.objects.filter(
        group=farm_registry_group
    ).count()
    if total_count == 0:
        logger.warning('DCAS: No farm registry farm registry group')
        return

    # run pipeline
    request_date = current_dt.date()
    if dcas_config.get('override_request_date', None):
        request_date = datetime.date.fromisoformat(
            dcas_config.get('override_request_date')
        )
    pipeline = DCASDataPipeline(
        farm_registry_group, request_date,
        crop_num_partitions=dcas_config.get('farm_npartitions', None),
        grid_crop_num_partitions=dcas_config.get(
            'grid_crop_npartitions', None
        )
    )
    pipeline.run()

    store_csv = dcas_config.get('store_csv', False)
    if (
        store_csv and
        os.path.exists(pipeline.data_output.output_csv_file_path)
    ):
        s3_storage = default_storage
        with open(pipeline.data_output.output_csv_file_path) as content:
            out_path = os.path.join(
                'dcas_csv',
                os.path.basename(pipeline.data_output.output_csv_file_path)
            )
            s3_storage.save(out_path, content)


@shared_task(name='log_farms_without_messages')
def log_farms_without_messages(request_id=None, chunk_size=1000):
    """
    Celery task to log farms without messages using chunked queries.

    :param request_id: Id for the pipeline output
    :type request_id: int
    :param chunk_size: Number of rows to process per iteration
    :type chunk_size: int
    """
    try:
        # Get the most recent DCAS request
        dcas_request = DCASRequest.objects.get(
            id=request_id
        )

        # Initialize pipeline output to get the directory path
        dcas_output = DCASPipelineOutput(request_id)
        parquet_path = dcas_output._get_directory_path(
            dcas_output.DCAS_OUTPUT_DIR + '/*.parquet'
        )

        # Query farms without messages in chunks
        for df_chunk in DataQuery.get_farms_without_messages(
            parquet_path, chunk_size=chunk_size
        ):
            if df_chunk.empty:
                logger.info(
                    "No farms found with missing messages in this chunk."
                )
                continue

            # Log missing messages in the database
            error_logs = []
            for _, row in df_chunk.iterrows():
                try:
                    farm = Farm.objects.get(id=row['farm_id'])
                except Farm.DoesNotExist:
                    logger.warning(
                        f"Farm ID {row['farm_id']} not found, skipping."
                    )
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

            # Bulk insert logs per chunk to optimize database writes
            if error_logs:
                DCASErrorLog.objects.bulk_create(error_logs)
                logger.info(
                    f"Logged {len(error_logs)} farms with missing messages."
                )

    except DCASRequest.DoesNotExist:
        logger.error(f"No DCASRequest found for request_id {request_id}.")
    except Exception as e:
        logger.error(f"Error processing missing messages: {str(e)}")
