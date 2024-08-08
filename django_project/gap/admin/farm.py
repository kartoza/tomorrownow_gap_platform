# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""
import json

from django.contrib import admin, messages

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm
)
from spw.generator import calculate_from_point


@admin.action(description='Generate Farm SPW')
def generate_farm_spw(modeladmin, request, queryset):
    """Restart plumber process action."""
    if queryset.count() > 1:
        modeladmin.message_user(
            request,
            'Just able to select 1 farm!',
            messages.SUCCESS
        )
    elif queryset.count() == 1:
        output, history_dict = calculate_from_point(
            queryset.first().geometry
        )
        history_dict.update(output.data.__dict__)
        modeladmin.message_user(
            request,
            json.dumps(history_dict, indent=4),
            messages.SUCCESS
        )


@admin.register(FarmCategory)
class FarmCategoryAdmin(AbstractDefinitionAdmin):
    """FarmCategory admin."""

    pass


@admin.register(FarmRSVPStatus)
class FarmRSVPStatusAdmin(AbstractDefinitionAdmin):
    """FarmRSVPStatus admin."""

    pass


@admin.register(Farm)
class FarmAdmin(admin.ModelAdmin):
    """Admin for Farm."""

    list_display = (
        'unique_id', 'latitude', 'longitude',
        'rsvp_status', 'category', 'crop'
    )
    search_fields = ('unique_id',)
    filter = ('unique_id',)
    list_filter = ('rsvp_status', 'category', 'crop')
    actions = (generate_farm_spw,)

    def latitude(self, obj: Farm):
        """Latitude of farm."""
        return obj.geometry.y

    def longitude(self, obj: Farm):
        """Longitude of farm."""
        return obj.geometry.x
