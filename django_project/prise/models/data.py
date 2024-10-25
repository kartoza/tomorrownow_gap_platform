# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Data prise models.
"""

from datetime import datetime

from django.db import models
from django.utils.translation import gettext_lazy as _

from gap.models.farm import Farm
from prise.models.pest import Pest, PrisePest
from prise.variables import PriseDataType


class PriseDataByPestRawInput:
    """The raw input inserting to prise models."""

    pest: Pest = None
    value: float = None

    def __init__(
            self, pest_variable_name: str, value: float
    ):
        """Initialization.

        :param pest_variable_name:
            The variable name of pest, this will be mapped to Pest object.
            Can be checked on PrisePest
        :type pest_variable_name: str
        :param value: Value of the pest of data.
        :type value: float
        """
        self.pest = PrisePest.get_pest_by_variable_name(pest_variable_name)
        self.value = value


class PriseDataRawInput:
    """The raw input inserting to prise models."""

    def __init__(
            self, unique_id: str, generated_at: datetime,
            values: [PriseDataByPestRawInput],
            data_type: str = PriseDataType.NEAR_REAL_TIME,
    ):
        self.farm = Farm.get_farm_by_unique_id(unique_id)
        self.generated_at = generated_at
        self.data_type = data_type
        self.values = values


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
        unique_together = ('farm', 'generated_at', 'data_type')
        ordering = ('-generated_at',)
        db_table = 'prise_data'

    @staticmethod
    def insert_data(data: PriseDataRawInput):
        prise_data, _ = PriseData.objects.get_or_create(
            farm=data.farm,
            generated_at=data.generated_at,
            data_type=data.data_type,
        )
        for value in data.values:
            PriseDataByPest.objects.update_or_create(
                data=prise_data,
                pest=value.pest,
                defaults={
                    'value': value.value
                }
            )


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
        unique_together = ('data', 'pest')
        db_table = 'prise_data_by_pest'
