# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Load fixtures
"""

from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Command to load fixtures."""

    help = 'Generate country geometry'

    def handle(self, *args, **options):
        """Handle load fixtures."""
        call_command('loaddata', 'gap/fixtures/1.country.json')
