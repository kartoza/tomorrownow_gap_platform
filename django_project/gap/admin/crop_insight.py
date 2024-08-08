# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admins
"""
from django.contrib import admin

from core.admin import AbstractDefinitionAdmin
from gap.models import (
    Crop
)


@admin.register(Crop)
class CropAdmin(AbstractDefinitionAdmin):
    """Crop admin."""

    pass
