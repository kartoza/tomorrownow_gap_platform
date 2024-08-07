# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Attribute, Country, Provider, Measurement, Station, IngestorSession,
    IngestorSessionProgress, Dataset, DatasetAttribute, DataSourceFile,
    DatasetType, Unit, Crop
)


@admin.register(Unit)
class UnitAdmin(AbstractDefinitionAdmin):
    """Unit admin."""

    pass


@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    """Attribute admin."""

    list_display = (
        'name', 'description', 'variable_name', 'unit',
    )
    search_fields = ('name',)


@admin.register(Country)
class CountryAdmin(AbstractDefinitionAdmin):
    """Country admin."""

    pass


@admin.register(Provider)
class ProviderAdmin(AbstractDefinitionAdmin):
    """Provider admin."""

    pass


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
        'name', 'provider', 'type', 'time_step',
        'store_type', 'is_internal_use'
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


@admin.register(DataSourceFile)
class DataSourceFileAdmin(admin.ModelAdmin):
    """DataSourceFile admin."""

    list_display = (
        'name', 'dataset', 'format', 'start_date_time',
        'end_date_time', 'created_on'
    )
    list_filter = ('dataset',)


@admin.register(Crop)
class CropAdmin(AbstractDefinitionAdmin):
    """Crop admin."""

    pass
