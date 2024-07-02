# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models for NetCDF Datasets
"""

from django.contrib.gis.db import models

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
    other_definitions = models.JSONField(default=dict)


class NetCDFFile(models.Model):
    """Model representing a NetCDF file that is stored in S3 Storage."""

    name = models.CharField(
        max_length=512
    )
    provider = models.ForeignKey(
        Provider, on_delete=models.CASCADE
    )
    start_date_time = models.DateTimeField()
    end_date_time = models.DateTimeField()
    dmrpp_path = models.CharField(
        max_length=512, null=True, blank=True
    )
    local_path = models.CharField(
        max_length=512, null=True, blank=True
    )
