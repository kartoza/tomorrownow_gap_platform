# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM ingestor.
"""

import json
import logging
import datetime
import pytz
import traceback
from math import ceil
import xarray as xr
import numpy as np
import pandas as pd
from xarray.core.dataset import Dataset as xrDataset
from django.utils import timezone


from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus
)
from gap.providers import CBAMNetCDFReader
from gap.utils.zarr import BaseZarrReader


logger = logging.getLogger(__name__)


class CBAMIngestor:

    def __init__(self, session: IngestorSession):
        self.session = session
        self.dataset = Dataset.objects.get(name='CBAM Climate Reanalysis')
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.datasource_file, self.created = DataSourceFile.objects.get_or_create(
            name='cbam.zarr',
            dataset=self.dataset,
            format=DatasetStore.ZARR,
            defaults={
                'created_on': timezone.now(),
                'start_date_time': timezone.now(),
                'end_date_time': timezone.now() - datetime.timedelta(days=20 * 365)
            }
        )
        # min+max are the BBOX that GAP processes
        # inc and original_min comes from CBAM netcdf file
        self.lat_metadata = {
            'min': -27,
            'max': 16,
            'inc': 0.03574368,
            'original_min': -12.5969
        }
        self.lon_metadata = {
            'min': 21.8,
            'max': 52,
            'inc': 0.036006329,
            'original_min': 26.9665
        }
        self.reindex_tolerance = 0.001
        self.existing_dates = None

    def find_start_latlng(self, metadata: dict):
        diff = ceil(abs((metadata['original_min'] - metadata['min'])/metadata['inc']))
        return metadata['original_min'] - (diff * metadata['inc'])

    def is_date_in_zarr(self, date: datetime.date):
        if self.created:
            return False
        if self.existing_dates is None:
            reader = BaseZarrReader(self.dataset, [], None, None, None)
            reader.setup_reader()
            ds = reader.open_dataset(self.datasource_file)
            self.existing_dates = ds.date.values
        np_date = np.datetime64(f'{date.isoformat()}')
        return np_date in self.existing_dates

    def store_as_zarr(self, dataset: xrDataset, date: datetime.date):
        new_date = pd.date_range(f'{date.isoformat()}', periods=1)
        dataset = dataset.assign_coords(date=new_date)
        del dataset.attrs['Date']
        # Generate the new latitude and longitude arrays
        min_lat = self.find_start_latlng(self.lat_metadata)
        min_lon = self.find_start_latlng(self.lon_metadata)
        new_lat = np.arange(min_lat, self.lat_metadata['max'] + self.lat_metadata['inc'], self.lat_metadata['inc'])
        new_lon = np.arange(min_lon, self.lon_metadata['max'] + self.lon_metadata['inc'], self.lon_metadata['inc'])
        expanded_ds = dataset.reindex(lat=new_lat, lon=new_lon, method='nearest', tolerance=self.reindex_tolerance)
        zarr_url = BaseZarrReader.get_zarr_base_url(self.s3) + self.datasource_file.name
        if self.created:
            self.created = False
            expanded_ds.to_zarr(zarr_url, mode='w', consolidated=True, storage_options=self.s3_options, encoding={
                'date': {
                    'units': f'days since {date.isoformat()}'
                }
            })
        else:
            expanded_ds.to_zarr(zarr_url, mode='a', append_dim='date', consolidated=True, storage_options=self.s3_options)

    def _run(self):
        self.metadata = {
            'start_date': None,
            'end_date': None,
            'total_processed': 0
        }
        sources = DataSourceFile.objects.filter(
            dataset=self.dataset,
            format=DatasetStore.NETCDF
        ).order_by('start_date_time')
        logger.info(f'Total CBAM source files: {sources.count()}')
        if not sources.exists():
            return
        source_reader = CBAMNetCDFReader(self.dataset, [], None, None, None)
        source_reader.setup_reader()
        total_monthyear = 0
        progress = None
        curr_monthyear = None
        for source in sources:
            iter_monthyear = source.start_date_time.date()
            # check if iter_monthyear is already in dataset
            if self.is_date_in_zarr(iter_monthyear):
                continue
            if curr_monthyear is None:
                self.metadata['start_date'] = iter_monthyear
                curr_monthyear = iter_monthyear
                progress = IngestorSessionProgress.objects.create(
                    session=self.session,
                    filename=f'{curr_monthyear.year}-{curr_monthyear.month}',
                    row_count=0
                )
            source_ds = source_reader.open_dataset(source)
            # merge source_ds to target zarr
            self.store_as_zarr(source_ds, iter_monthyear)
            self.metadata['total_processed'] += 1
            total_monthyear += 1
            if curr_monthyear.year != iter_monthyear.year and curr_monthyear.month != iter_monthyear.month:
                # update ingestion progress
                if progress:
                    progress.row_count = total_monthyear
                    progress.status = IngestorSessionStatus.SUCCESS
                    progress.save()
                # reset vars
                total_monthyear = 0
                curr_monthyear = iter_monthyear
                progress = IngestorSessionProgress.objects.create(
                    session=self.session,
                    filename=f'{curr_monthyear.year}-{curr_monthyear.month}',
                    row_count=0
                )
        # save last progress
        if progress:
            progress.row_count = total_monthyear
            progress.status = IngestorSessionStatus.SUCCESS
            progress.save()
            self.metadata['end_date'] = iter_monthyear

    def run(self):
        # Run the ingestion
        try:
            self._run()
            self.notes = json.dumps(self.metadata, default=str)
            logger.info(f'Ingestor CBAM NetCDFFile: {self.notes}')
            # update datasourcefile
            if self.metadata['start_date'] and self.metadata['start_date'] < self.datasource_file.start_date_time.date():
                self.datasource_file.start_date_time = datetime.datetime.combine(
                    self.metadata['start_date'], datetime.time.min, tzinfo=pytz.UTC
                )
            if self.metadata['end_date'] and self.metadata['end_date'] > self.datasource_file.end_date_time.date():
                self.datasource_file.end_date_time = datetime.datetime.combine(
                    self.metadata['end_date'], datetime.time.min, tzinfo=pytz.UTC
                )
            self.datasource_file.save()
        except Exception as e:
            logger.error('Ingestor CBAM failed!', e)
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass
