# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Load fixtures
"""

import logging
import os

from django.core.management import call_command
from django.core.management.base import BaseCommand

from core.settings.utils import DJANGO_ROOT

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Command to load fixtures."""

    help = 'Generate country geometry'

    def handle(self, *args, **options):
        """Handle load fixtures."""
        folder = os.path.join(
            DJANGO_ROOT, 'gap', 'fixtures'
        )
        for subdir, dirs, files in os.walk(folder):
            files.sort()
            for file in files:
                if file.endswith('.json'):
                    logger.info(f"Loading {file}")
                    print(f"Loading {file}")
                    call_command('loaddata', file)
