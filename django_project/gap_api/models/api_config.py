# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for API Config
"""

from django.db import models

from gap.models import DatasetType


class DatasetTypeAPIConfig(models.Model):
    """API Config based on DatasetType."""

    type = models.OneToOneField(
        DatasetType, on_delete=models.CASCADE,
        primary_key=True,
    )
    max_daterange = models.IntegerField(
        default=-1,
        help_text='Maximum date range that API can fetch data.'
    )

    def __str__(self):
        return f'{self.type.variable_name}'
