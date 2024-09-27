# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Station, StationType
)


@admin.register(StationType)
class StationTypeAdmin(AbstractDefinitionAdmin):
    """Station admin."""

    pass


@admin.register(Station)
class StationAdmin(admin.ModelAdmin):
    """Station admin."""

    list_display = (
        'code', 'name', 'station_type', 'country', 'provider'
    )
    list_filter = ('provider', 'station_type', 'country')
    search_fields = ('code', 'name')
