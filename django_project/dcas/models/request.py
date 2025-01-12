# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Config
"""

from django.contrib.gis.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from gap.models.common import Country


class DCASRequest(models.Model):
    """Model representing a request to generate DCAS data."""

    requested_at = models.DateTimeField(
        default=timezone.now,
        help_text="The time when the request was created."
    )
    country = models.ForeignKey(
        Country, on_delete=models.CASCADE
    )
    start_time = models.DateTimeField(
        null=True,
        blank=True
    )
    end_time = models.DateTimeField(
        null=True,
        blank=True
    )

    class Meta:  # noqa
        db_table = 'dcas_request'
        verbose_name = _('Request')
        ordering = ['-requested_at']
