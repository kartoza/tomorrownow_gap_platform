# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Output
"""

from django.utils import timezone
from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

from core.models.common import Definition
from core.models.background_task import TaskStatus
from gap.models.common import Country
from dcas.models.request import DCASRequest


class DCASOutput(models.Model):
    """Model to track the delivery of file output to SFTP."""

    request = models.ForeignKey(
        DCASRequest, on_delete=models.CASCADE,
        related_name='output',
        help_text="The DCAS request associated with this output."
    )
    file_name = models.CharField(
        max_length=255,
        null=True,
        blank=True
    )
    delivered_at = models.DateTimeField(
        default=timezone.now,
        null=True,
        blank=True,
        help_text="The time when the file was delivered."
    )
    status = models.CharField(
        max_length=255,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING,
        null=True,
        blank=True,
        help_text="The delivery status of the file."
    )

    class Meta:
        db_table = 'dcas_output'
        verbose_name = 'DCAS Output'
        verbose_name_plural = 'DCAS Outputs'
        ordering = ['-delivered_at']
