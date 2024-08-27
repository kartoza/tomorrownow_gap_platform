# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Salient ingestor.
"""

import os
import uuid
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
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus,
    CollectorSession, Preferences
)
from gap.ingestor.base import BaseIngestor
from gap.utils.netcdf import NetCDFMediaS3
from gap.utils.zarr import BaseZarrReader


logger = logging.getLogger(__name__)


class SalientCollector(BaseIngestor):
    """Collector for Salient seasonal forecast data."""

    def __init__(self, session: CollectorSession, working_dir: str = '/tmp'):
        """Initialize SalientCollector."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        self.s3 = NetCDFMediaS3.get_s3_variables('salient')
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': NetCDFMediaS3.get_s3_client_kwargs()
        }
        self.total_count = 0
        self.data_files = []

    def _get_date_config(self):
        """Retrieve date from config or default to be today."""
        date_str = '-today'
        if 'forecast_date' in self.session.additional_config:
            date_str = self.session.additional_config
        return date_str

    def _get_variable_list_config(self):
        """Retrieve variable list."""
        default_vars = [
            "precip",  # precipitation (mm/day)
            "tmin",  # minimum daily temperature (degC)
            "tmax",  # maximum daily temperature (degC)
            "wspd",  # wind speed at 10m (m/s)
            "tsi",  # total solar insolation (W/m^2)
            "rh",  # relative humidity (%),
            "temp"
        ]
        return self.session.additional_config.get(
            'variable_list', default_vars)

    def _get_coords(self):
        """Retrieve polygon coordinates."""
        # TODO: we need to verify on dev whether 
        # it's possible to request for whole GAP AoI
        return self.session.additional_config.get(
            'coords',
            list(Preferences.load().area_of_interest.coords[0])
        )

    def _convert_forecast_date(self, date_str: str):
        """Convert string forecast date to date object.

        :param date_str: '-today' or 'YYYY-MM-DD'
        :type date_str: str
        :return: date object
        :rtype: Date object
        """
        if date_str == '-today':
            today = datetime.datetime.now()
            return today.date()
        dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return dt.date()

    def _store_as_netcdf_file(self, file_path: str, date_str: str):
        """Store the downscale Salient NetCDF to Default's Storage.
        
        Data will be truncated to 3 months data.
        :param file_path: file path to downscale netcdf
        :type file_path: str
        :param date_str: forecast date in str
        :type date_str: str
        """
        # prepare start and end dates
        forecast_date = self._convert_forecast_date(date_str)
        end_date = forecast_date + datetime.timedelta(days=3 * 30)

        # open the original netcdf file
        dataset = xr.open_dataset(file_path)

        # select 3 months data
        sliced_ds = dataset.sel(
            forecast_day=slice(
                forecast_date.isoformat(), end_date.isoformat()
            )
        )

        # store as netcdf to S3
        filename = f'{str(uuid.uuid4())}.nc'
        netcdf_url = (
            NetCDFMediaS3.get_netcdf_base_url(self.s3) + filename
            
        )
        sliced_ds.to_netcdf(
            netcdf_url, engine='netcdf4', mode='w',
            storage_options=self.s3_options
        )

        # create DataSourceFile
        start_datetime = datetime.datetime(
            forecast_date.year, forecast_date.month, forecast_date.day,
            0, 0, 0, tzinfo=pytz.UTC
        )
        end_datetime = datetime.datetime(
            end_date.year, end_date.month, end_date.day,
            0, 0, 0, tzinfo=pytz.UTC
        )
        self.data_files.append(DataSourceFile.objects.create(
            name=filename,
            dataset=self.dataset,
            start_date_time=start_datetime,
            end_date_time=end_datetime,
            created_on=timezone.now(),
            format=DatasetStore.NETCDF
        ))
        self.total_count += 1
        self.session.dataset_files.set(self.data_files)

    def _run(self):
        """Download Salient Seasonal Forecast from sdk."""
        logger.info(f'Running data collector for Salient.')
        logger.info(f'Working directory: {self.working_dir}')

        # initialize sdk
        sk.set_file_destination(self.working_dir)
        sk.login(
            os.environ.get("SALIENT_SDK_USERNAME"),
            os.environ.get("SALIENT_SDK_PASSWORD")
        )

        # create the requested locations
        loc = sk.Location(shapefile=sk.upload_shapefile(
            coords=self._get_coords(),
            geoname="gap-1",
            force=False)
        )

        # request data
        fcst_file = sk.downscale(
            loc=loc,
            variables=self._get_variable_list_config(),
            date=self._get_date_config(),
            members=50,
            force=False,
            verbose=False,
        )

        self._store_as_netcdf_file(fcst_file, self._get_date_config())

    def run(self):
        """Run Salient Data Collector."""
        try:
            self._run()
        except Exception as e:
            logger.error('Collector Salient failed!', e)
            logger.error(traceback.format_exc())
            raise Exception(e)
        finally:
            pass


class SalientIngestor(BaseIngestor):
    """Ingestor for Salient seasonal forecast data."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize SalientIngestor."""
        super().__init__(session, working_dir)
        self.dataset = Dataset.objects.get(name='Salient Seasonal Forecast')
        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.metadata = {}
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

    def _run(self):
        """Run Salient ingestor."""
        # Query the datasource file

        today = datetime.datetime.now()
        # self.store_as_zarr(fcst_file, today.date())

        # update start/end date of datasource file

    def store_as_zarr(self, dataset: xrDataset, forecast_date: datetime.date):
        forecast_dates = pd.date_range(
            f'{forecast_date.isoformat()}', periods=1)
        end_date = forecast_date + datetime.timedelta(days=3 * 30)
        data_vars = {}
        for var_name, da in dataset.data_vars.items():    
            # Initialize the data_var with the empty array
            data_vars[var_name] = da.expand_dims('forecast_date', axis=0)

        # Create the dataset
        zarr_ds = xr.Dataset(
            data_vars, 
            coords={
                'forecast_date': ('forecast_date', forecast_dates),
                'forecast_day': (
                    'forecast_day', dataset.coords['forecast_day'].values
                ),
                'lat': ('lat', dataset.coords['lat'].values),
                'lon': ('lon', dataset.coords['lon'].values),
                'ensemble': ('ensemble', np.arange(50)),
            }
        )

        # transform forecast_day into number of days
        fd = np.datetime64(forecast_date.isoformat())
        forecast_day_idx = (zarr_ds['forecast_day'] - fd).dt.days.data
        zarr_ds = zarr_ds.assign_coords(
            forecast_day_idx=("forecast_day", forecast_day_idx))
        zarr_ds = zarr_ds.swap_dims({'forecast_day': 'forecast_day_idx'})
        zarr_ds = zarr_ds.drop_vars('forecast_day')

        # TODO: expand lat and lon

        # write to zarr
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        if self.created:
            zarr_ds.to_zarr(
                zarr_url, mode='w', consolidated=True,
                storage_options=self.s3_options
            )
        else:
            zarr_ds.to_zarr(
                zarr_url, mode='a', append_dim='forecast_date',
                consolidated=True,
                storage_options=self.s3_options
            )

    def run(self):
        """Run Salient Ingestor."""
        # Run the ingestion
        try:
            self._run()
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
