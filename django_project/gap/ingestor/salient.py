# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient ingestor.
"""

import os
import json
import logging
import datetime
import pytz
import traceback
import tempfile
from math import ceil
import numpy as np
import pandas as pd
import xarray as xr
import salientsdk as sk
from xarray.core.dataset import Dataset as xrDataset
from django.utils import timezone
import fsspec


from gap.models import (
    Dataset, DataSourceFile, DatasetStore,
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus
)
from gap.utils.zarr import BaseZarrReader


logger = logging.getLogger(__name__)


class SalientIngestor:
    """Ingestor for Salient seasonal forecast data."""

    def __init__(self, session: IngestorSession):
        """Initialize SalientIngestor."""
        self.session = session
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.datasource_file, self.created = (
            DataSourceFile.objects.get_or_create(
                name='salient.zarr',
                dataset=self.dataset,
                format=DatasetStore.ZARR,
                defaults={
                    'created_on': timezone.now(),
                    'start_date_time': timezone.now(),
                    'end_date_time': (
                        timezone.now() + datetime.timedelta(days=3 * 30)
                    )
                }
            )
        )
        self.metadata = {}

    def _run(self, tmpdirname):
        sk.set_file_destination(tmpdirname)
        sk.login(os.environ.get("SALIENT_SDK_USERNAME"), os.environ.get("SALIENT_SDK_PASSWORD"))
        loc = sk.Location(shapefile=sk.upload_shapefile(
            coords=[(33.38, -0.64), (36.05, -0.64), (36.05, 4.25), (33.38, 4.25), (33.38, -0.64)],
            geoname="gap-1",
            force=False)
        )
        var = [
            "precip",  # precipitation (mm/day)
            # "tmin",  # minimum daily temperature (degC)
            # "tmax",  # maximum daily temperature (degC)
            # "wspd",  # wind speed at 10m (m/s)
            # "tsi",  # total solar insolation (W/m^2)
            "rh",  # relative humidity (%)
        ]
        fcst_file = sk.downscale(
            loc=loc,
            variables=var,
            date="-today",  # can pass a vector of dates to backfill historical
            members=50,  # ensemble count
            force=False,  # set True to bypass caching and force a refresh
            verbose=False,  # set True to see diagnostic information
        )
        # fcst_file = '/tmp/tmp_feqjipy/downscale_3cd8597ca65ceb479f60332453b4e59b.nc' # 21
        # fcst_file = '/tmp/tmp_feqjipy/downscale_567b20fb87fbf487f6bed05e8afce0dd.nc' # 22
        fcst = xr.open_dataset(fcst_file)
        # print(fcst)

        today = datetime.datetime.now()
        self.store_as_zarr(fcst_file, today.date())

        # self.store_as_zarr(fcst, datetime.date(2024, 8, 21))
        # self.store_as_zarr(fcst, datetime.date(2024, 8, 22))

    def store_as_zarr(self, dataset: xrDataset, forecast_date: datetime.date):
        forecast_dates = pd.date_range(f'{forecast_date.isoformat()}', periods=1)
        end_date = forecast_date + datetime.timedelta(days=3 * 30)
        ds = dataset.sel(forecast_day=slice(forecast_date.isoformat(), end_date.isoformat()))
        print(ds)
        data_vars = {}
        for var_name, da in ds.data_vars.items():    
            # Initialize the data_var with the empty array
            data_vars[var_name] = da.expand_dims('forecast_date', axis=0)
        # Create the dataset
        zarr_ds = xr.Dataset(
            data_vars, 
            coords={
                'forecast_date': ('forecast_date', forecast_dates),
                'forecast_day': ('forecast_day', ds.coords['forecast_day'].values),
                'lat': ('lat', ds.coords['lat'].values),
                'lon': ('lon', ds.coords['lon'].values),
                'ensemble': ('ensemble', np.arange(50)),
            }
        )
        # transform forecast_day into number of days
        fd = np.datetime64(forecast_date.isoformat())
        forecast_day_idx = (zarr_ds['forecast_day'] - fd).dt.days.data
        zarr_ds = zarr_ds.assign_coords(forecast_day_idx=("forecast_day", forecast_day_idx))
        zarr_ds = zarr_ds.swap_dims({'forecast_day': 'forecast_day_idx'})
        zarr_ds = zarr_ds.drop_vars('forecast_day')
        # write to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        # zarr_ds.to_zarr(
        #     zarr_url, mode='w', consolidated=True,
        #     storage_options=self.s3_options
        # )
        zarr_ds.to_zarr(
            zarr_url, mode='a', append_dim='forecast_date', consolidated=True,
            storage_options=self.s3_options
        )

    def run(self):
        """Run Salient Ingestor."""
        # Run the ingestion
        try:
            with tempfile.TemporaryDirectory(delete=False) as tmpdirname:
                self._run(tmpdirname)
            self.notes = json.dumps(self.metadata, default=str)
            logger.info(f'Ingestor Salient: {self.notes}')
        except Exception as e:
            logger.error('Ingestor Salient failed!', e)
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass

    def verify(self):
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        self.zarr_ds = xr.open_zarr(s3_mapper, consolidated=True)
        print(self.zarr_ds)
