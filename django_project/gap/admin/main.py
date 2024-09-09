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
    DatasetType, Unit, Village, CollectorSession
)
from gap.tasks.collector import run_collector_session
from gap.tasks.ingestor import run_ingestor_session


@admin.register(Unit)
class UnitAdmin(AbstractDefinitionAdmin):
    """Unit admin."""

    pass


@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    """Attribute admin."""

    list_display = (
        'id', 'name', 'description', 'variable_name', 'unit',
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
        'id', 'dataset', 'attribute', 'source', 'source_unit',
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


@admin.action(description='Run Ingestor Session Task')
def trigger_ingestor_session(modeladmin, request, queryset):
    """Run Ingestor Session."""
    for query in queryset:
        run_ingestor_session(query.id)


@admin.register(IngestorSession)
class IngestorSessionAdmin(admin.ModelAdmin):
    """IngestorSession admin."""

    list_display = (
        'run_at', 'status', 'end_at', 'ingestor_type'
    )
    list_filter = ('ingestor_type', 'status')
    inlines = (IngestorSessionProgressInline,)
    actions = (trigger_ingestor_session,)


@admin.action(description='Run Collector Session Task')
def trigger_collector_session(modeladmin, request, queryset):
    """Run Collector Session."""
    for query in queryset:
        run_collector_session.delay(query.id)


@admin.register(CollectorSession)
class CollectorSessionAdmin(admin.ModelAdmin):
    """CollectorSession admin."""

    list_display = (
        'run_at', 'status', 'end_at', 'ingestor_type',
        'total_output'
    )
    list_filter = ('ingestor_type', 'status')
    actions = (trigger_collector_session,)

    def total_output(self, obj: CollectorSession):
        """Return total count."""
        return obj.dataset_files.count()


@admin.register(DataSourceFile)
class DataSourceFileAdmin(admin.ModelAdmin):
    """DataSourceFile admin."""

    list_display = (
        'name', 'dataset', 'format', 'start_date_time',
        'end_date_time', 'created_on'
    )
    list_filter = ('dataset',)


@admin.register(Village)
class VillageAdmin(AbstractDefinitionAdmin):
    """Village admin."""

    pass
