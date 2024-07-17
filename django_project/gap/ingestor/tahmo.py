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
    Provider, Station, ObservationType, Country,
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus,
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
        dataset_attrs_by_key = {}

        # INGEST STATIONS
        for (dirpath, dirnames, filenames) in os.walk(dir_path):
            for filename in filenames:
                try:
                    if 'station' in filename.lower():
                        reader = csv.DictReader(
                            open(os.path.join(dirpath, filename), 'r')
                        )
                        rows = list(reader)
                        progress = IngestorSessionProgress.objects.create(
                            session=self.session,
                            filename=filename,
                            row_count=len(rows)  # noqa
                        )
                        for data in rows:
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
                        progress.status = IngestorSessionStatus.SUCCESS
                        progress.save()
                except UnicodeDecodeError as e:
                    continue

        # INGEST MEASUREMENTS
        for (dirpath, dirnames, filenames) in os.walk(dir_path):
            for filename in filenames:
                code = filename.split('_')[0]
                try:
                    if 'station' in filename.lower():
                        continue
                    station = Station.objects.get(
                        code=code, provider=self.provider
                    )
                    reader = csv.DictReader(
                        open(os.path.join(dirpath, filename), 'r')
                    )
                    rows = list(reader)
                    progress = IngestorSessionProgress.objects.create(
                        session=self.session,
                        filename=filename,
                        row_count=len(rows)  # noqa
                    )
                    measurements = []
                    for data in rows:
                        date = data['Timestamp']  # noqa
                        if not date:
                            continue

                        # Check the datetime
                        try:
                            try:
                                date_time = datetime.strptime(
                                    date, '%Y-%m-%d'
                                ).replace(tzinfo=timezone.utc)
                            except ValueError:
                                date_time = datetime.strptime(
                                    date, '%m/%d/%y'
                                ).replace(tzinfo=timezone.utc)

                        except ValueError as e:
                            progress.status = IngestorSessionStatus.FAILED
                            progress.notes = json.dumps({
                                'filename': filename,
                                'error': f'{e}'
                            })
                            progress.save()
                            continue

                        date_time.replace(second=0)

                        # Save all data
                        for key, value in data.items():  # noqa
                            try:
                                # Get the dataset attributes
                                attr = None
                                try:
                                    attr = dataset_attrs_by_key[key]
                                except KeyError:
                                    pass

                                if not attr:
                                    try:
                                        attr = DatasetAttribute.objects.get(
                                            dataset=self.dataset,
                                            source=key
                                        )
                                    except DatasetAttribute.DoesNotExist:
                                        continue

                                dataset_attrs_by_key[key] = attr

                                # Skip empty one
                                if value == '':
                                    continue

                                # Save the measurements
                                measurements.append(
                                    Measurement(
                                        station_id=station.id,
                                        dataset_attribute=attr,
                                        date_time=date_time,
                                        value=float(value)
                                    )
                                )
                            except (KeyError, ValueError) as e:
                                raise Exception(
                                    json.dumps({
                                        'filename': filename,
                                        'data': data,
                                        'error': f'{e}'
                                    })
                                )
                    Measurement.objects.bulk_create(
                        measurements, ignore_conflicts=True
                    )
                    progress.status = IngestorSessionStatus.SUCCESS
                    progress.save()
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
