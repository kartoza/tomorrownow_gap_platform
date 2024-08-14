# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Lookup table
"""

from django.db import models

from django.db.models import Q


class RainfallClassification(models.Model):
    """Rainfall classification."""

    name = models.CharField(
        max_length=512,
        unique=True,
    )
    min_value = models.FloatField(
        null=True, blank=True
    )
    max_value = models.FloatField(
        null=True, blank=True
    )

    @staticmethod
    def classify(value):
        """Classify rainfall."""
        return RainfallClassification.objects.filter(
            Q(min_value__lte=value, max_value__gte=value) |
            Q(min_value__lte=value, max_value__isnull=True) |
            Q(min_value__isnull=True, max_value__gte=value)

        ).first()
