# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Pest
)


@admin.register(Pest)
class PestAdmin(AbstractDefinitionAdmin):
    """Pest admin."""

    list_display = (
        'name', 'scientific_name', 'taxonomic_rank', 'link'
    )
    list_filter = ('taxonomic_rank',)
