# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models for NetCDF Datasets
"""

import os
import boto3
from django.contrib.gis.db import models
from django.conf import settings
from django.dispatch import receiver

from gap.models.station import Provider, ObservationType
from gap.models.measurement import Attribute


class NetCDFProviderMetadata(models.Model):
    """Model that stores metadata for NetCDF Provider."""

    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    metadata = models.JSONField(
        default=dict
    )


class NetCDFProviderAttribute(models.Model):
    """Model that stores attribute in NetCDF files."""

    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    attribute = models.ForeignKey(
        Attribute, on_delete=models.CASCADE
    )
    observation_type = models.ForeignKey(
        ObservationType, on_delete=models.CASCADE
    )
    unit = models.CharField(
        max_length=512, null=True, blank=True
    )
    variable_name = models.CharField(
        max_length=512
    )
    other_definitions = models.JSONField(
        default=dict,
        blank=True,
        null=True
    )


class NetCDFFile(models.Model):
    """Model representing a NetCDF file that is stored in S3 Storage."""

    name = models.CharField(
        max_length=512,
        help_text="Filename with its path in the object storage (S3)"
    )
    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    start_date_time = models.DateTimeField()
    end_date_time = models.DateTimeField()
    dmrpp_path = models.CharField(
        max_length=512, null=True, blank=True,
        help_text="DMR++ path in the object storage (S3)"
    )
    local_path = models.CharField(
        max_length=512, null=True, blank=True,
        help_text="Relative path to the local file cache"
    )
    created_on = models.DateTimeField()

    @property
    def has_dmrpp(self) -> bool:
        """Check if NetCDFFile has DMR++ file.

        :return: True if the object has DMR++ file
        :rtype: bool
        """
        return self.dmrpp_path is not None

    @property
    def opendap_url(self) -> str:
        """Get the URL for this file in the Hyrax Server.

        :return: URL to access the file using opendap
        :rtype: str
        """
        if not self.has_cached_file():
            self._store_to_file_cache()
        return f"{settings.OPENDAP_BASE_URL}{self.local_path}"

    @property
    def cached_file_path(self) -> str:
        """Get file path to the cached NetCDFFile.

        :return: file path of DMR++ file or original file
        :rtype: str
        """
        if self.local_path is None:
            return None
        return os.path.join(
            settings.OPENDAP_FILE_CACHE_DIR,
            self.local_path
        )

    def has_cached_file(self) -> bool:
        """Check whether the NetCDFFile has been cached.

        :return: True if cached file exists.
        :rtype: bool
        """
        file_path = self.cached_file_path
        if file_path is None:
            return False
        return os.path.exists(file_path)

    def _store_to_file_cache(self):
        """Store NetCDFFile to local file cache.

        If DMR++ file is not available, then store the original file.
        """
        remote_file_path = None
        if self.has_dmrpp:
            self.local_path = f'{self.name}.dmrpp'
            remote_file_path = self.dmrpp_path
        else:
            self.local_path = self.name
            remote_file_path = self.name
        full_path = self.cached_file_path
        if os.path.exists(full_path):
            return
        dir_path = os.path.dirname(full_path)
        os.makedirs(dir_path, exist_ok=True)
        boto3_client = boto3.client('s3')
        boto3_client.download_file(
            os.environ.get('S3_AWS_BUCKET_NAME'),
            remote_file_path,
            full_path,
            Config=settings.AWS_TRANSFER_CONFIG
        )
        self.save(update_fields=['local_path'])


@receiver(models.signals.post_delete, sender=NetCDFFile)
def auto_delete_file_on_delete(sender, instance: NetCDFFile, **kwargs):
    """Delete file from filesystem.

    when corresponding `NetCDFFile` object is deleted.
    """
    if instance.has_cached_file():
        os.remove(instance.cached_file_path)
