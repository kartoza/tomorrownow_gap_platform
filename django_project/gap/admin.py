# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from .models import (
    Attribute, Country, Provider, Measurement, Station,
    IngestorSession, IngestorSessionProgress,
    Dataset, DatasetAttribute, NetCDFFile, DatasetType, Unit
)


@admin.register(Unit)
class UnitAdmin(admin.ModelAdmin):
    """Unit admin."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)


@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    """Attribute admin."""

    list_display = (
        'name', 'description', 'variable_name', 'unit',
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


@admin.register(DatasetType)
class DatasetTypeAdmin(admin.ModelAdmin):
    """DatasetType admin."""

    list_display = (
        'name', 'type'
    )


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    """Dataset admin."""

    list_display = (
        'name', 'provider', 'type', 'time_step', 'store_type',
    )


@admin.register(DatasetAttribute)
class DatasetAttributeAdmin(admin.ModelAdmin):
    """DatasetAttribute admin."""

    list_display = (
        'dataset', 'attribute', 'source', 'source_unit',
    )
    list_filter = ('dataset',)


@admin.register(Measurement)
class MeasurementAdmin(admin.ModelAdmin):
    """Measurement admin."""

    list_display = (
        'station', 'dataset_attribute', 'date_time', 'value'
    )
    list_filter = ('station',)
    search_fields = ('name',)


@admin.register(Station)
class StationAdmin(admin.ModelAdmin):
    """Station admin."""

    list_display = (
        'code', 'name', 'country', 'provider'
    )
    list_filter = ('provider', 'country')
    search_fields = ('code', 'name')


class IngestorSessionProgressInline(admin.TabularInline):
    """IngestorSessionProgress inline."""

    model = IngestorSessionProgress
    extra = 0


@admin.register(IngestorSession)
class IngestorSessionAdmin(admin.ModelAdmin):
    """IngestorSession admin."""

    list_display = (
        'run_at', 'status', 'end_at', 'ingestor_type'
    )
    list_filter = ('ingestor_type', 'status')
    inlines = (IngestorSessionProgressInline,)


@admin.register(NetCDFFile)
class NetCDFFileAdmin(admin.ModelAdmin):
    """NetCDFFile admin."""

    list_display = (
        'name', 'dataset', 'start_date_time', 'end_date_time', 'created_on'
    )
    list_filter = ('dataset',)
