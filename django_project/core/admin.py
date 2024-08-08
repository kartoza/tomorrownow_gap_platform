"""
Tomorrow Now GAP.

.. note:: Definition admin
"""
from django.contrib import admin


class AbstractDefinitionAdmin(admin.ModelAdmin):
    """Abstract admin for definition."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)
