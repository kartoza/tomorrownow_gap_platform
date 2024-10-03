# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Station, StationType, StationHistory
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


@admin.register(StationHistory)
class StationHistoryAdmin(admin.ModelAdmin):
    """Station admin."""

    list_display = (
        'station', 'provider', 'latitude', 'longitude', 'altitude', 'date_time'
    )
    list_filter = ('station__provider', 'station')

    def provider(self, obj: StationHistory):
        """Return provider."""
        return obj.station.provider

    def latitude(self, obj: StationHistory):
        """Return latitude."""
        return obj.geometry.y

    def longitude(self, obj: StationHistory):
        """Return latitude."""
        return obj.geometry.x
