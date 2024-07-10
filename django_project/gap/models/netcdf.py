# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models for NetCDF Datasets
"""

from django.contrib.gis.db import models

from gap.models.dataset import Dataset


class NetCDFFile(models.Model):
    """Model representing a NetCDF file that is stored in S3 Storage."""

    name = models.CharField(
        max_length=512,
        help_text="Filename with its path in the object storage (S3)"
    )
    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE
    )
    start_date_time = models.DateTimeField()
    end_date_time = models.DateTimeField()
    created_on = models.DateTimeField()
