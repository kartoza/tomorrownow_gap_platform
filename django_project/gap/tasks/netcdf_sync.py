# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tasks for NetCDF File Sync
"""

import os
from typing import Tuple
from celery.utils.log import get_task_logger
from datetime import datetime
import pytz
import s3fs
from django.utils import timezone

from core.celery import app
from gap.models import (
    Attribute,
    Provider,
    NetCDFFile,
    Dataset,
    DatasetAttribute,
    DatasetStore,
    DatasetTimeStep,
    DatasetType,
    Unit,
    CastType
)
from gap.utils.netcdf import (
    NetCDFProvider,
    CBAM_VARIABLES,
    SALIENT_VARIABLES,
)


logger = get_task_logger(__name__)


def initialize_provider(provider_name: str) -> Tuple[Provider, Dataset]:
    """Initialize provider object for NetCDF.

    :param provider_name: provider name
    :type provider_name: str
    :param metadata: provider metadata
    :type metadata: dict
    :return: provider and dataset object
    :rtype: Tuple[Provider, Dataset]
    """
    provider, _ = Provider.objects.get_or_create(name=provider_name)
    if provider.name == NetCDFProvider.CBAM:
        dataset_type, _ = DatasetType.objects.get_or_create(
            name='Climate Reanalysis',
            defaults={
                'type': CastType.HISTORICAL
            }
        )
    else:
        dataset_type, _ = DatasetType.objects.get_or_create(
            name='Seasonal Forecast',
            defaults={
                'type': CastType.FORECAST
            }
        )
    dataset, _ = Dataset.objects.get_or_create(
        name=f'{provider.name} {dataset_type.name}',
        provider=provider,
        type=dataset_type,
        defaults={
            'time_step': DatasetTimeStep.DAILY,
            'store_type': DatasetStore.NETCDF
        }
    )
    return provider, dataset


def initialize_provider_variables(dataset: Dataset, variables: dict):
    """Initialize NetCDF Attribute for given dataset.

    :param provider: dataset object
    :type provider: Dataset
    :param variables: Variable names
    :type variables: dict
    """
    for key, val in variables.items():
        unit, _ = Unit.objects.get_or_create(
            name=val.unit
        )
        attr, _ = Attribute.objects.get_or_create(
            name=val.name,
            unit=unit,
            variable_name=key,
            defaults={
                'description': val.desc
            }
        )
        DatasetAttribute.objects.get_or_create(
            dataset=dataset,
            attribute=attr,
            source=key,
            source_unit=unit
        )


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
            if NetCDFFile.objects.filter(name=file_path).exists():
                continue
            netcdf_filename = os.path.split(file_path)[1]
            file_date = datetime.strptime(
                netcdf_filename.split('.')[0], '%Y-%m-%d')
            start_datetime = datetime(
                file_date.year, file_date.month, file_date.day,
                0, 0, 0, tzinfo=pytz.UTC
            )
            NetCDFFile.objects.create(
                name=file_path,
                dataset=dataset,
                start_date_time=start_datetime,
                end_date_time=start_datetime,
                created_on=timezone.now()
            )
            count += 1
    if count > 0:
        logger.info(f'{dataset.name} - Added new NetCDFFile: {count}')


@app.task(name="netcdf_s3_sync")
def netcdf_s3_sync():
    """Sync NetCDF Files from S3 storage."""
    _, cbam_dataset = initialize_provider(NetCDFProvider.CBAM)
    _, salient_dataset = initialize_provider(NetCDFProvider.SALIENT)
    initialize_provider_variables(cbam_dataset, CBAM_VARIABLES)
    initialize_provider_variables(salient_dataset, SALIENT_VARIABLES)
    sync_by_dataset(cbam_dataset)
    sync_by_dataset(salient_dataset)
