# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS AppConfig
"""
from django.apps import AppConfig


class DcasConfig(AppConfig):
    """App Config for DCAS."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'dcas'
