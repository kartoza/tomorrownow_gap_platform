"""
Tomorrow Now GAP.

.. note:: Config for Permission Module
"""
from django.apps import AppConfig


class PermissionConfig(AppConfig):
    """Config for Permission App."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'permission'

    def ready(self):
        """App ready handler."""
        from permission.group_default_permission import group_gap_default, post_save_user_signal_handler  # noqa
