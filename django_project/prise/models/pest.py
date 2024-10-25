# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message prise models.
"""

from django.db import models

from gap.models.crop_insight import Pest


class PrisePest(models.Model):
    """Pest specifically for Prise."""

    pest = models.OneToOneField(
        Pest, on_delete=models.CASCADE, unique=True
    )
    variable_name = models.CharField(
        max_length=256,
        help_text='Pest variable name that being used on CABI PRISE CSV.'
    )
