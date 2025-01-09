"""
Tomorrow Now GAP GDD.

.. note:: Admins
"""
from django.contrib import admin
from gap.models import GDDConfig, GDDMatrix


@admin.register(GDDConfig)
class GDDConfigAdmin(admin.ModelAdmin):
    """Admin interface for GDDConfig."""
    
    list_display = ('crop', 'base_temperature', 'cap_temperature', 'config')
    list_filter = ('config', 'crop')


@admin.register(GDDMatrix)
class GDDMatrixAdmin(admin.ModelAdmin):
    """Admin interface for GDDMatrix."""
    
    list_display = ('crop', 'growth_stage', 'gdd_threshold', 'config')
    list_filter = ('crop', 'growth_stage', 'config')
