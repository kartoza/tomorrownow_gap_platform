# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: App Config for GAP API
"""

from django.apps import AppConfig


class GapApiConfig(AppConfig):
    """App Config for GAP API."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'gap_api'
