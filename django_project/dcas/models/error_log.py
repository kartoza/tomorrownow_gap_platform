# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Output
"""

from django.utils import timezone
from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

from gap.models.farm import Farm
from dcas.models.request import DCASRequest


class DCASErrorLog(models.Model):
    """Model to store farms that cannot be processed."""

    request = models.ForeignKey(
        DCASRequest, on_delete=models.CASCADE,
        related_name='error_logs',
        help_text="The DCAS request associated with this error."
    )
    farm_id = models.ForeignKey(
        Farm, on_delete=models.CASCADE,
        help_text="The unique identifier of the farm that failed to process."
    )
    error_message = models.TextField(
        help_text="Details about why the farm could not be processed."
    )
    logged_at = models.DateTimeField(
        default=timezone.now,
        help_text="The time when the error was logged."
    )

    class Meta:
        db_table = 'dcas_error_log'
        verbose_name = _('Error Log')
        ordering = ['-logged_at']
