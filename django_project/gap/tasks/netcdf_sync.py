# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tasks for NetCDF File Sync
"""

import os
from celery.utils.log import get_task_logger
from datetime import datetime
import pytz
import s3fs
from django.utils import timezone

from core.celery import app
from gap.models import (
    DataSourceFile,
    Dataset,
    DatasetStore
)
from gap.utils.netcdf import (
    NetCDFProvider,
)


logger = get_task_logger(__name__)


def sync_by_dataset(dataset: Dataset):
    """Synchronize NetCDF files in s3 storage.

    :param provider: dataset object
    :type provider: Dataset
    """
    s3_variables = NetCDFProvider.get_s3_variables(dataset.provider)
    directory_path = s3_variables.get('AWS_DIR_PREFIX')
    fs = s3fs.S3FileSystem(
        key=s3_variables.get('AWS_ACCESS_KEY_ID'),
        secret=s3_variables.get('AWS_SECRET_ACCESS_KEY'),
        client_kwargs=NetCDFProvider.get_s3_client_kwargs(dataset.provider)
    )
    logger.info(f'Check NETCDF Files by dataset {dataset.name}')
    bucket_name = s3_variables.get('AWS_BUCKET_NAME')
    count = 0
    for dirpath, dirnames, filenames in \
        fs.walk(f's3://{bucket_name}/{directory_path}'):
        for filename in filenames:
            if not filename.endswith('.nc'):
                continue
            cleaned_dir = dirpath.replace(
                f'{bucket_name}/{directory_path}', '')
            if cleaned_dir:
                file_path = (
                    f'{cleaned_dir}{filename}' if
                    cleaned_dir.endswith('/') else
                    f'{cleaned_dir}/{filename}'
                )
            else:
                file_path = filename
            if file_path.startswith('/'):
                file_path = file_path[1:]
            check_exist = DataSourceFile.objects.filter(
                name=file_path,
                dataset=dataset,
                format=DatasetStore.NETCDF
            ).exists()
            if check_exist:
                continue
            netcdf_filename = os.path.split(file_path)[1]
            file_date = datetime.strptime(
                netcdf_filename.split('.')[0], '%Y-%m-%d')
            start_datetime = datetime(
                file_date.year, file_date.month, file_date.day,
                0, 0, 0, tzinfo=pytz.UTC
            )
            DataSourceFile.objects.create(
                name=file_path,
                dataset=dataset,
                start_date_time=start_datetime,
                end_date_time=start_datetime,
                created_on=timezone.now(),
                format=DatasetStore.NETCDF
            )
            count += 1
    if count > 0:
        logger.info(f'{dataset.name} - Added new NetCDFFile: {count}')
    return count


@app.task(name="netcdf_s3_sync")
def netcdf_s3_sync():
    """Sync NetCDF Files from S3 storage."""
    cbam_dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
    total_count = sync_by_dataset(cbam_dataset)
    if total_count > 0:
        # run ingestor to convert into zarr
        pass
