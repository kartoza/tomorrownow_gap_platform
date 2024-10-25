# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Data prise models.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _

from gap.models.farm import Farm
from prise.models.pest import Pest
from prise.variables import PriseDataType


class PriseData(models.Model):
    """Model that contains prise data."""

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    ingested_at = models.DateTimeField(
        auto_now_add=True,
        help_text='Date and time at which the prise data was ingested.'
    )
    generated_at = models.DateTimeField(
        help_text='Date and time at which the prise data was generated.'
    )
    data_type = models.CharField(
        default=PriseDataType.NEAR_REAL_TIME,
        choices=(
            (PriseDataType.NEAR_REAL_TIME, _(PriseDataType.NEAR_REAL_TIME)),
        ),
        max_length=512
    )

    class Meta:  # noqa
        ordering = ('-generated_at',)
        db_table = 'prise_data'


class PriseDataByPest(models.Model):
    """Model that contains prise data per pest."""

    data = models.ForeignKey(
        PriseData, on_delete=models.CASCADE
    )
    pest = models.ForeignKey(
        Pest, on_delete=models.CASCADE
    )
    value = models.FloatField()

    class Meta:  # noqa
        db_table = 'prise_data_by_pest'
