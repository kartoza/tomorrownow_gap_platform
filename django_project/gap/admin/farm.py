# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""

from django.contrib import admin, messages

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm
)
from gap.tasks.crop_insight import generate_spw


@admin.action(description='Generate farms spw')
def generate_farm_spw(modeladmin, request, queryset):
    """Generate Farms SPW."""
    generate_spw.delay(list(queryset.values_list('id', flat=True)))
    modeladmin.message_user(
        request,
        'Process will be started in background!',
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
