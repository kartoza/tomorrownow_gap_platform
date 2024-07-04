# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tasks for NetCDF File Sync
"""

import os
from celery.utils.log import get_task_logger
from datetime import datetime
from pydap.client import open_url
from pydap.model import BaseType
import boto3
from django.utils import timezone
from django.contrib.gis.geos import Point

from core.celery import app
from gap.models import (
    Country,
    ObservationType,
    Attribute,
    Measurement,
    Provider,
    Station,
    NetCDFFile,
    NetCDFProviderMetadata,
    NetCDFProviderAttribute
)
from gap.utils.netcdf import (
    NetCDFProvider,
    CBAM_VARIABLES,
    SALIENT_VARIABLES,
)


logger = get_task_logger(__name__)


def initialize_provider(provider_name: str, metadata: dict) -> Provider:
    """Initialize provider object for NetCDF.

    :param provider_name: provider name
    :type provider_name: str
    :param metadata: provider metadata
    :type metadata: dict
    :return: provider object
    :rtype: Provider
    """
    provider, created = Provider.objects.get_or_create(name=provider_name)
    if created:
        NetCDFProviderMetadata.objects.create(
            provider=provider,
            metadata=metadata
        )
    return provider


def initialize_provider_variables(provider: Provider, variables: dict):
    """Initialize NetCDFProviderAttribute for given provider.

    :param provider: provider object
    :type provider: Provider
    :param variables: Variable names
    :type variables: dict
    """
    obs_type, _ = ObservationType.objects.get_or_create(
        name='Satellite Observations'
    )
    for key, val in variables.items():
        attr, _ = Attribute.objects.get_or_create(
            name=val.name,
            defaults={
                'description': val.desc
            }
        )
        NetCDFProviderAttribute.objects.get_or_create(
            provider=provider,
            attribute=attr,
            observation_type=obs_type,
            variable_name=key,
            defaults={
                'unit': val.unit
            }
        )


def _check_key_exists(client, file_path):
    """Check whether key exists in s3 storage.

    :param client: s3 client
    :type client: boto3.client
    :param file_path: file_path as key
    :type file_path: str
    :return: True if file_path exists in s3
    :rtype: bool
    """
    try:
        response = client.list_objects_v2(
            Bucket=os.environ.get('S3_AWS_BUCKET_NAME'), Prefix=file_path)
        for obj in response.get('Contents', []):
            if file_path == obj['Key']:
                return True
        return False  # no keys match
    except KeyError:
        return False  # no keys found
    except Exception:
        # Handle or log other exceptions such as bucket doesn't exist
        return False


def sync_by_provider(provider: Provider):
    """Synchronize NetCDF files by Provider in s3 storage.

    :param provider: provider object
    :type provider: Provider
    """
    directory_path = f'{provider.name.lower()}/'
    dmrpp_path = f'{directory_path}dmrpp'
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=os.environ.get('S3_AWS_BUCKET_NAME'),
        Prefix=directory_path,
        Delimiter="/"
    )

    count = 0
    for page in pages:
        for obj in page.get('Contents', []):
            file_path = obj['Key']
            if NetCDFFile.objects.filter(name=file_path).exists():
                continue
            netcdf_filename = os.path.split(file_path)[1]
            dmrpp_file_path = os.path.join(
                dmrpp_path, f'{netcdf_filename}.dmrpp')
            dmrpp_exists = _check_key_exists(client, dmrpp_file_path)
            date_str = netcdf_filename.split('.')[0]
            NetCDFFile.objects.create(
                name=file_path,
                provider=provider,
                start_date_time=datetime.strptime(date_str, '%Y-%m-%d'),
                end_date_time=datetime.strptime(date_str, '%Y-%m-%d'),
                dmrpp_path=dmrpp_file_path if dmrpp_exists else None,
                created_on=timezone.now()
            )
            count += 1
    if count > 0:
        logger.info(f'Added new NetCDFFile: {count}')


@app.task(name="netcdf_s3_sync")
def netcdf_s3_sync():
    """Sync NetCDF Files from S3 storage."""
    cbam = initialize_provider(
        NetCDFProvider.CBAM,
        {
            'lon': {
                'min': 26.9665,
                'inc': 0.036006329,
                'size': 475
            },
            'lat': {
                'min': -12.5969,
                'inc': 0.03574368,
                'size': 539
            }
        }
    )
    salient = initialize_provider(
        NetCDFProvider.SALIENT,
        {
            'lon': {
                'min': 28.875,
                'inc': 0.25,
                'size': 9
            },
            'lat': {
                'min': -2.875,
                'inc': 0.25,
                'size': 8
            }
        }
    )
    initialize_provider_variables(cbam, CBAM_VARIABLES)
    initialize_provider_variables(salient, SALIENT_VARIABLES)
    sync_by_provider(cbam)
    sync_by_provider(salient)


@app.task(name="netcdf_readall")
def netcdf_readall(netcdffile_id):
    obs_type, _ = ObservationType.objects.get_or_create(
        name='Satellite Observations'
    )
    country = Country.objects.first()
    netcdf = NetCDFFile.objects.get(id=netcdffile_id)
    dataset = open_url(netcdf.opendap_url)
    lat_array = dataset.lat[:].data
    lon_array = dataset.lon[:].data
    measurements = {}
    for variable in CBAM_VARIABLES.keys():
        attr_data = dataset[variable]
        print(f'{variable} - has shape: {attr_data.shape}')
        if isinstance(attr_data, BaseType):
            arr_data = attr_data
        else:
            arr_data = attr_data[variable]
        attribute_val = CBAM_VARIABLES[variable]
        attr, _ = Attribute.objects.get_or_create(
            name=attribute_val.name,
            defaults={
                'description': attribute_val.desc
            }
        )
        measurements[variable] = {
            'data': arr_data[0, :, :],
            'attribute': attr,
            'unit': attribute_val.unit
        }
        Measurement.objects.filter(
            station__provider=netcdf.provider,
            attribute=attr,
            date_time=netcdf.start_date_time
        ).delete()
    point_idx = 1
    total_measurements = 0
    for idx_lon, lon in enumerate(lon_array):
        for idx_lat, lat in enumerate(lat_array):
            p = Point(x=lon, y=lat)
            netcdfpoint_id = f'CBAM-{netcdffile_id}-{point_idx}'
            station, _ = Station.objects.get_or_create(
                geometry=p,
                provider=netcdf.provider,
                observation_type=obs_type,
                defaults={
                    'code': netcdfpoint_id,
                    'country': country,
                    'name': netcdfpoint_id
                }
            )
            results = []
            for variable in CBAM_VARIABLES.keys():
                results.append(Measurement(
                    station=station,
                    attribute=measurements[variable]['attribute'],
                    unit=measurements[variable]['unit'],
                    date_time=netcdf.start_date_time,
                    value=measurements[variable]['data'][0][idx_lat][idx_lon].data
                ))
            if results:
                Measurement.objects.bulk_create(results)
                total_measurements += len(results)
                if total_measurements % 1000 == 0:
                    print(f'Created measurements {total_measurements}')
            point_idx += 1
