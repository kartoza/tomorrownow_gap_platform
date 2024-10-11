# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Base Ingestor.
"""

from typing import Union
import logging
import datetime
import pytz
import uuid
import fsspec
import xarray as xr

from django.utils import timezone
from django.core.files.storage import default_storage
from django.db import transaction

from core.models import BackgroundTask
from gap.models import (
    CollectorSession,
    IngestorSession,
    IngestorSessionStatus,
    Dataset,
    DatasetStore,
    DataSourceFile,
    DataSourceFileCache
)
from gap.utils.zarr import BaseZarrReader


logger = logging.getLogger(__name__)


class BaseIngestor:
    """Collector/Ingestor Base class."""

    def __init__(
        self,
        session: Union[CollectorSession, IngestorSession],
        working_dir: str
    ):
        """Initialize ingestor/collector."""
        self.session = session
        self.working_dir = working_dir

    def is_cancelled(self):
        """Check if session is cancelled by user.

        This method will refetch the session object from DB.
        :return: True if session is gracefully cancelled.
        :rtype: bool
        """
        self.session.refresh_from_db()
        return self.session.is_cancelled

    def get_config(self, name: str, default_value = None):
        """Get config from session.

        :param name: config name
        :type name: str
        :param default_value: default value if config does not exist,
            defaults to None
        :type default_value: any, optional
        :return: config value or default_value
        :rtype: any
        """
        if self.session.additional_config is None:
            return default_value
        return self.session.additional_config.get(name, default_value)


class BaseZarrIngestor(BaseIngestor):
    """Base Ingestor class for Zarr product."""

    default_zarr_name = f'{uuid.uuid4()}.zarr'

    def __init__(self, session, working_dir):
        """Initialize base zarr ingestor."""
        super().__init__(session, working_dir)
        self.dataset = self._init_dataset()

        self.s3 = BaseZarrReader.get_s3_variables()
        self.s3_options = {
            'key': self.s3.get('AWS_ACCESS_KEY_ID'),
            'secret': self.s3.get('AWS_SECRET_ACCESS_KEY'),
            'client_kwargs': BaseZarrReader.get_s3_client_kwargs()
        }
        self.metadata = {}

        # get zarr data source file
        datasourcefile_id = self.get_config('datasourcefile_id')
        if datasourcefile_id:
            self.datasource_file = DataSourceFile.objects.get(
                id=datasourcefile_id)
            self.created = not self.get_config(
                'datasourcefile_zarr_exists', True)
        else:
            datasourcefile_name = self.get_config(
                'datasourcefile_name', f'{self.default_zarr_name}')
            self.datasource_file, self.created = (
                DataSourceFile.objects.get_or_create(
                    name=datasourcefile_name,
                    dataset=self.dataset,
                    format=DatasetStore.ZARR,
                    defaults={
                        'created_on': timezone.now(),
                        'start_date_time': timezone.now(),
                        'end_date_time': (
                            timezone.now()
                        )
                    }
                )
            )

    def _init_dataset(self) -> Dataset:
        """Fetch dataset for this ingestor.

        :raises NotImplementedError: should be implemented in child class
        :return: Dataset for this ingestor
        :rtype: Dataset
        """
        raise NotImplementedError

    def _update_zarr_source_file(self, updated_date: datetime.date):
        """Update zarr DataSourceFile start and end datetime.

        :param updated_date: Date that has been processed
        :type updated_date: datetime.date
        """
        if self.created:
            self.datasource_file.start_date_time = datetime.datetime(
                updated_date.year, updated_date.month, updated_date.day,
                0, 0, 0, tzinfo=pytz.UTC
            )
            self.datasource_file.end_date_time = (
                self.datasource_file.start_date_time
            )
        else:
            if self.datasource_file.start_date_time.date() > updated_date:
                self.datasource_file.start_date_time = datetime.datetime(
                    updated_date.year, updated_date.month,
                    updated_date.day,
                    0, 0, 0, tzinfo=pytz.UTC
                )
            if self.datasource_file.end_date_time.date() < updated_date:
                self.datasource_file.end_date_time = datetime.datetime(
                    updated_date.year, updated_date.month,
                    updated_date.day,
                    0, 0, 0, tzinfo=pytz.UTC
                )
        self.datasource_file.save()

    def _remove_temporary_source_file(
            self, source_file: DataSourceFile, file_path: str):
        """Remove temporary file from collector.

        :param source_file: Temporary File
        :type source_file: DataSourceFile
        :param file_path: s3 file path
        :type file_path: str
        """
        try:
            default_storage.delete(file_path)
        except Exception as ex:
            logger.error(
                f'Failed to remove original source_file {file_path}!', ex)
        finally:
            source_file.delete()

    def _open_zarr_dataset(self, drop_variables = []) -> xr.Dataset:
        """Open existing Zarr file.

        :param drop_variables: variables to exclude from reader
        :type drop_variables: list, optional
        :return: xarray dataset
        :rtype: xr.Dataset
        """
        zarr_url = (
            BaseZarrReader.get_zarr_base_url(self.s3) +
            self.datasource_file.name
        )
        s3_mapper = fsspec.get_mapper(zarr_url, **self.s3_options)
        return xr.open_zarr(
            s3_mapper, consolidated=True, drop_variables=drop_variables)

    def verify(self):
        """Verify the resulting zarr file."""
        self.zarr_ds = self._open_zarr_dataset()
        print(self.zarr_ds)

    def _invalidate_zarr_cache(self):
        """Invalidate existing zarr cache after ingestor is finished."""
        source_caches = DataSourceFileCache.objects.select_for_update().filter(
            source_file=self.datasource_file
        )
        with transaction.atomic():
            for source_cache in source_caches:
                source_cache.expired_on = timezone.now()
                source_cache.save()


def ingestor_revoked_handler(bg_task: BackgroundTask):
    """Event handler when ingestor task is cancelled by celery.

    :param bg_task: background task
    :type bg_task: BackgroundTask
    """
    # retrieve ingestor session
    session = IngestorSession.objects.filter(
        id=int(bg_task.context_id)
    ).first()
    if session is None:
        return

    # update status as cancelled
    session.status = IngestorSessionStatus.CANCELLED
    session.save(update_fields=['status'])
