# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from .models import (
    Attribute, Country, Provider, Measurement, Station, IngestorSession,
    NetCDFProviderMetadata, NetCDFProviderAttribute, NetCDFFile
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


@admin.register(NetCDFProviderMetadata)
class NetCDFProviderMetadataAdmin(admin.ModelAdmin):
    """NetCDFProviderMetadata admin."""

    list_display = (
        'provider'
    )


@admin.register(NetCDFProviderAttribute)
class NetCDFProviderAttributeAdmin(admin.ModelAdmin):
    """NetCDFProviderAttribute admin."""

    list_display = (
        'provider', 'attribute', 'observation_type', 'unit'
    )
    list_filter = ('provider', 'observation_type')


@admin.register(NetCDFFile)
class NetCDFFileAdmin(admin.ModelAdmin):
    """NetCDFFile admin."""

    list_display = (
        'name', 'provider', 'start_date_time', 'end_date_time', 'created_on'
    )
    list_filter = ('provider')
