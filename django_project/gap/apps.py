# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: App Config
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class GAPConfig(AppConfig):
    """App Config for GroundObservations."""

    name = 'gap'
    verbose_name = _('Global Access Platform')

    def ready(self):
        """App ready handler."""
        from gap.tasks.crop_insight import generate_crop_plan  # noqa
        pass
