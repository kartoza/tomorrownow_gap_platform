# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Admin for DCAS Models
"""

from django.contrib import admin

from dcas.models import (
    DCASConfig,
    DCASConfigCountry,
    DCASRule,
    GDDConfig,
    GDDMatrix
)


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
