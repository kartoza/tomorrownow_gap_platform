# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Prise admins
"""
from django.contrib import admin

from prise.models import PriseMessage, PrisePest


@admin.register(PriseMessage)
class PriseMessageAdmin(admin.ModelAdmin):
    """Admin page for PriseMessage."""

    list_display = ('pest', 'farm_group', 'message_count')

    filter_horizontal = ('messages',)

    def message_count(self, obj: PriseMessage):
        """Message count."""
        return obj.messages.count()


@admin.register(PrisePest)
class PrisePestAdmin(admin.ModelAdmin):
    """Admin page for PrisePest."""

    list_display = ('pest', 'variable_name')
