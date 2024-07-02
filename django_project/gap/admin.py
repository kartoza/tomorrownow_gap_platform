# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from .models import (
    Attribute, Country, Provider, Measurement, Station, IngestorSession
)


@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    """Attribute admin."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)


@admin.register(Country)
class CountryAdmin(admin.ModelAdmin):
    """Country admin."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)


@admin.register(Provider)
class ProviderAdmin(admin.ModelAdmin):
    """Provider admin."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)


@admin.register(Measurement)
class MeasurementAdmin(admin.ModelAdmin):
    """Measurement admin."""

    list_display = (
        'station', 'attribute', 'date_time', 'value'
    )
    list_filter = ('station', 'attribute')
    search_fields = ('name',)


@admin.register(Station)
class StationAdmin(admin.ModelAdmin):
    """Station admin."""

    list_display = (
        'code', 'name', 'country', 'provider'
    )
    list_filter = ('provider', 'country')
    search_fields = ('code', 'name')


@admin.register(IngestorSession)
class IngestorSessionAdmin(admin.ModelAdmin):
    """IngestorSession admin."""

    list_display = (
        'run_at', 'status', 'end_at', 'ingestor_type'
    )
    list_filter = ('ingestor_type', 'status')
