# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admin Preferences

"""
from django.contrib import admin

from gap.models import Preferences

admin.site.register(Preferences, admin.ModelAdmin)
