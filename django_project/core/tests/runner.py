# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Test runner.
"""

from django.conf import settings
from django.test.runner import DiscoverRunner

from core.celery import app as celery_app


class CustomTestRunner(DiscoverRunner):
    """Postgres schema test runner."""

    @staticmethod
    def __disable_celery():
        """Disabling celery."""
        settings.CELERY_BROKER_URL = \
            celery_app.conf.BROKER_URL = 'filesystem:///dev/null/'
        celery_app.conf.task_always_eager = True
        data = {
            'data_folder_in': '/tmp',
            'data_folder_out': '/tmp',
            'data_folder_processed': '/tmp',
        }
        settings.BROKER_TRANSPORT_OPTIONS = \
            celery_app.conf.BROKER_TRANSPORT_OPTIONS = data

    def setup_test_environment(self, **kwargs):
        """Prepare test env."""
        CustomTestRunner.__disable_celery()
        super(CustomTestRunner, self).setup_test_environment(**kwargs)
