# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Message prise models.
"""

from django.db import models

from gap.models.crop_insight import Pest
from message.models import MessageTemplate


class PriseMessage(models.Model):
    """Model that stores message template linked with pest."""

    pest = models.ForeignKey(
        Pest, on_delete=models.CASCADE
    )
    messages = models.ManyToManyField(
        MessageTemplate, null=True, blank=True
    )

    def __str__(self):
        """Return string representation."""
        return self.pest.name
