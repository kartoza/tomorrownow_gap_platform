# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Project level settings.
"""

from .prod import *  # noqa

TEST_RUNNER = 'core.tests.runner.CustomTestRunner'
DEBUG = True

# Disable caching while in development
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.dummy.DummyCache',
    }
}
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'
