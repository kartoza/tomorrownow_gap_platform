# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Admin for DCAS Models
"""

from django.contrib import admin

from dcas.models import (
    DCASConfig,
    DCASConfigCountry
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
