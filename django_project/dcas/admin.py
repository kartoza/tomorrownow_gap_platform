# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Admin for DCAS Models
"""

from import_export.admin import ExportMixin
from import_export_celery.admin_actions import create_export_job_action
from django.contrib import admin

from dcas.models import (
    DCASConfig,
    DCASConfigCountry,
    DCASRule,
    DCASRequest,
    DCASOutput,
    DCASErrorLog,
    GDDConfig,
    GDDMatrix
)
from dcas.resources import DCASErrorLogResource


class ConfigByCountryInline(admin.TabularInline):
    """Inline list for config by country."""

    model = DCASConfigCountry
    extra = 0


@admin.register(DCASConfig)
class DCASConfigAdmin(admin.ModelAdmin):
    """Admin page for DCASConfig."""

    list_display = ('name', 'description', 'is_default')
    inlines = (ConfigByCountryInline,)


@admin.register(DCASRule)
class DCASRuleAdmin(admin.ModelAdmin):
    """Admin page for DCASRule."""

    list_display = (
        'crop', 'crop_stage_type', 'crop_growth_stage',
        'parameter', 'min_range', 'max_range', 'code'
    )
    list_filter = (
        'crop', 'crop_stage_type', 'crop_growth_stage',
        'parameter'
    )


@admin.register(DCASRequest)
class DCASRequestAdmin(admin.ModelAdmin):
    """Admin page for DCASRequest."""

    list_display = ('requested_at', 'country', 'start_time', 'end_time')
    list_filter = ('country',)


@admin.register(DCASOutput)
class DCASOutputAdmin(admin.ModelAdmin):
    """Admin page for DCASOutput."""

    list_display = ('delivered_at', 'request', 'file_name', 'status')
    list_filter = ('request', 'status')


@admin.register(DCASErrorLog)
class DCASErrorLogAdmin(ExportMixin, admin.ModelAdmin):
    """Admin class for DCASErrorLog model."""

    resource_class = DCASErrorLogResource
    actions = [create_export_job_action]

    list_display = (
        "id",
        "request_id",
        "farm_id",
        "error_type",
        "error_message",
        "logged_at",
    )

    search_fields = ("error_message", "farm_id", "request__id")
    list_filter = ("error_type", "logged_at")

# GDD Config and Matrix


@admin.register(GDDConfig)
class GDDConfigAdmin(admin.ModelAdmin):
    """Admin interface for GDDConfig."""

    list_display = ('crop', 'base_temperature', 'cap_temperature', 'config')
    list_filter = ('config', 'crop')


@admin.register(GDDMatrix)
class GDDMatrixAdmin(admin.ModelAdmin):
    """Admin interface for GDDMatrix."""

    list_display = ('crop', 'crop_stage_type', 'gdd_threshold', 'config')
    list_filter = ('crop', 'crop_stage_type', 'config')
