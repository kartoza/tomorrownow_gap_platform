# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Collector utilities.
"""

import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta

import pytz
from django.conf import settings
from django.contrib.gis.geos import Polygon
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.utils import timezone

from core.utils import zip_folder_in_s3, remove_s3_folder
from gap.models import DataSourceFile, DatasetStore
from gap.providers import TomorrowIODatasetReader
from gap.providers.tio import tomorrowio_shortterm_forcast_dataset
from gap.utils.geometry import split_polygon_to_bbox
from gap.utils.reader import DatasetReaderInput, LocationInputType


def path(filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}tio-short-term-collector/{filename}'


def collect_sort_term_forecast_tio(polygon: Polygon, size: int):
    """Collect sort term forecast tio for all crop insights.

    :param size: In meters
    """
    s3_storage = default_storage
    dataset = tomorrowio_shortterm_forcast_dataset()
    today = datetime.now(tz=pytz.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_dt = today - timedelta(days=1)
    end_dt = today + timedelta(days=14)

    zip_file = path(f"{uuid.uuid4()}.zip")
    data_source_file, _ = DataSourceFile.objects.get_or_create(
        dataset=dataset,
        start_date_time=start_dt,
        end_date_time=end_dt,
        format=DatasetStore.ZIP_FILE,
        defaults={
            'name': zip_file,
            'created_on': timezone.now()
        }
    )
    filename = data_source_file.name.split('/')[-1]
    _uuid = os.path.splitext(filename)[0]
    zip_file = path(f"{_uuid}.zip")
    folder = path(_uuid)

    # If it is already have zip file, skip the process
    if s3_storage.exists(zip_file):
        return

    TomorrowIODatasetReader.init_provider()
    for bbox in split_polygon_to_bbox(polygon, size):

        # Check the file, skip if the file exist
        file_name = f"{hashlib.sha256(
            json.dumps(bbox.geojson).encode('utf-8')
        ).hexdigest()}.json"
        bbox_filename = os.path.join(folder, file_name)

        # If the json file is exist, skip it
        if s3_storage.exists(bbox_filename):
            continue

        # Get the data
        location_input = DatasetReaderInput(bbox, LocationInputType.POLYGON)
        reader = TomorrowIODatasetReader(
            dataset,
            dataset.datasetattribute_set.all(),
            location_input, start_dt, end_dt
        )
        reader.read()
        values = reader.get_data_values()

        # Save the reasult to file
        content = ContentFile(json.dumps(values.to_dict(), indent=4))
        s3_storage.save(bbox_filename, content)

    # Zip the folder
    zip_folder_in_s3(s3_storage, folder_path=folder, zip_file_name=zip_file)
    remove_s3_folder(s3_storage, folder)
