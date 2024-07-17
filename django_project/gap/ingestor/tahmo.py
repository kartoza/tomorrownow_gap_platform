# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo ingestor.
"""

import csv
import json
import os
import shutil
from datetime import datetime, timezone
from zipfile import ZipFile

from django.contrib.gis.geos import Point

from gap.ingestor.exceptions import FileNotFoundException
from gap.models import (
    Provider, Station, ObservationType, Country, IngestorSession,
    Measurement, Dataset, DatasetType, DatasetTimeStep,
    DatasetStore, DatasetAttribute
)


class TahmoVariable:
    """Contains Tahmo Variable."""

    def __init__(self, name, unit=None):
        """Initialize the Tahmo variable."""
        self.name = name
        self.unit = unit
        self.dataset_attr = None


class TahmoIngestor:
    """Ingestor for tahmo data."""

    def __init__(self, session: IngestorSession):
        """Initialize the ingestor."""
        self.session = session

        self.provider = Provider.objects.get(
            name='Tahmo'
        )
        self.obs_type = ObservationType.objects.get(
            name='Ground Observations'
        )
        self.dataset_type = DatasetType.objects.get(
            name='Ground Observational'
        )
        self.dataset, _ = Dataset.objects.get_or_create(
            name=f'Tahmo {self.dataset_type.name}',
            provider=self.provider,
            type=self.dataset_type,
            time_step=DatasetTimeStep.DAILY,
            store_type=DatasetStore.TABLE
        )

    def _run(self, dir_path):
        """Run the ingestor."""
        # Data is coming from CSV.
        # CSV headers:
        # - longitude
        # - latitude
        # - station code
        # - name

        # INGEST STATIONS
        for (dirpath, dirnames, filenames) in os.walk(dir_path):
            for filename in filenames:
                try:
                    reader = csv.DictReader(
                        open(os.path.join(dirpath, filename), 'r')
                    )
                    if 'station' in filename:
                        for data in reader:
                            try:
                                point = Point(
                                    x=float(data['longitude']),
                                    y=float(data['latitude']),
                                    srid=4326
                                )
                                try:
                                    country = Country.get_countries_by_point(
                                        point
                                    )[0]
                                except IndexError:
                                    country = None
                                Station.objects.get_or_create(
                                    code=data['station code'],
                                    provider=self.provider,
                                    defaults={
                                        'name': data['name'],
                                        'geometry': point,
                                        'country': country,
                                        'observation_type': self.obs_type,
                                    }
                                )
                            except KeyError as e:
                                raise Exception(
                                    json.dumps({
                                        'filename': filename,
                                        'data': data,
                                        'error': f'{e}'
                                    })
                                )
                except UnicodeDecodeError:
                    continue

        # INGEST MEASUREMENTS
        for (dirpath, dirnames, filenames) in os.walk(dir_path):
            for filename in filenames:
                code = filename.split('_')[0]
                try:
                    station = Station.objects.get(
                        code=code, provider=self.provider
                    )
                    reader = csv.DictReader(
                        open(os.path.join(dirpath, filename), 'r')
                    )
                    for data in reader:
                        date = data['']  # noqa
                        if not date:
                            continue
                        date_time = datetime.strptime(
                            date, '%Y-%m-%d %H:%M'
                        ).replace(tzinfo=timezone.utc)
                        date_time.replace(second=0)
                        for key, value in data.items():  # noqa
                            try:
                                dataset_attr = DatasetAttribute.objects.get(
                                    dataset=self.dataset,
                                    source=key
                                )
                            except DatasetAttribute.DoesNotExist:
                                continue
                            try:
                                # Skip empty one
                                if value == '':
                                    continue

                                measure, _ = Measurement.objects.get_or_create(
                                    station=station,
                                    dataset_attribute=dataset_attr,
                                    date_time=date_time,
                                    defaults={
                                        'value': float(value)
                                    }
                                )
                            except (KeyError, ValueError) as e:
                                raise Exception(
                                    json.dumps({
                                        'filename': filename,
                                        'data': data,
                                        'error': f'{e}'
                                    })
                                )
                except Station.DoesNotExist:
                    pass

    def run(self):
        """Run the ingestor."""
        if not self.session.file:
            raise FileNotFoundException()

        # Extract file
        dir_path = os.path.splitext(self.session.file.path)[0]
        with ZipFile(self.session.file.path, 'r') as zip_ref:
            zip_ref.extractall(dir_path)

        # Run the ingestion
        try:
            self._run(dir_path)
            shutil.rmtree(dir_path)
        except Exception as e:
            import traceback
            shutil.rmtree(dir_path)
            raise Exception(e)
