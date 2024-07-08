# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tasks for NetCDF File Sync
"""

import os
from typing import Tuple
from celery.utils.log import get_task_logger
from datetime import datetime
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
    Unit
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
        dataset_type = DatasetType.CLIMATE_REANALYSIS
    else:
        dataset_type = DatasetType.SEASONAL_FORECAST
    dataset, _ = Dataset.objects.get_or_create(
        name=provider.name,
        provider=provider,
        defaults={
            'type': dataset_type,
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
    directory_path = f'{dataset.provider.name.lower()}'
    fs = s3fs.S3FileSystem(client_kwargs={
        'endpoint_url': os.environ.get('AWS_ENDPOINT_URL')
    })
    bucket_name = os.environ.get('S3_AWS_BUCKET_NAME')
    count = 0
    for dirpath, dirnames, filenames in \
        fs.walk(f's3://{bucket_name}/{directory_path}'):
        for filename in filenames:
            if not dirpath.endswith(directory_path):
                continue
            cleaned_dir = dirpath.replace(f'{bucket_name}/', '')
            file_path = f'{cleaned_dir}/{filename}'
            if NetCDFFile.objects.filter(name=file_path).exists():
                continue
            netcdf_filename = os.path.split(file_path)[1]
            date_str = netcdf_filename.split('.')[0]
            NetCDFFile.objects.create(
                name=file_path,
                dataset=dataset,
                start_date_time=datetime.strptime(date_str, '%Y-%m-%d'),
                end_date_time=datetime.strptime(date_str, '%Y-%m-%d'),
                created_on=timezone.now()
            )
            count += 1
    if count > 0:
        logger.info(f'Added new NetCDFFile: {count}')


@app.task(name="netcdf_s3_sync")
def netcdf_s3_sync():
    """Sync NetCDF Files from S3 storage."""
    _, cbam_dataset = initialize_provider(NetCDFProvider.CBAM)
    _, salient_dataset = initialize_provider(NetCDFProvider.SALIENT)
    initialize_provider_variables(cbam_dataset, CBAM_VARIABLES)
    initialize_provider_variables(salient_dataset, SALIENT_VARIABLES)
    sync_by_dataset(cbam_dataset)
    sync_by_dataset(salient_dataset)
