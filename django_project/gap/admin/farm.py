# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farms admin
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    FarmCategory, FarmRSVPStatus, Farm
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

    def latitude(self, obj: Farm):
        """Latitude of farm."""
        return obj.geometry.y

    def longitude(self, obj: Farm):
        """Longitude of farm."""
        return obj.geometry.x
