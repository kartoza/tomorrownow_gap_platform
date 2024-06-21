# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: App Config
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class GroundObservationsConfig(AppConfig):
    """App Config for GroundObservations."""

    name = 'ground_observations'
    verbose_name = _('Ground Observations')
