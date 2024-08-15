# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from gap.models import (
    RainfallClassification
)


@admin.register(RainfallClassification)
class RainfallClassificationAdmin(admin.ModelAdmin):
    """RainfallClassification admin."""

    list_display = (
        'name', 'min_value', 'max_value'
    )
