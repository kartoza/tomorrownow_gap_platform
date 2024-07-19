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

STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
        "OPTIONS": {
            "location": "/home/web/media/default_test",
        },
    },
    "staticfiles": {
        "BACKEND": (
            "django.contrib.staticfiles.storage.ManifestStaticFilesStorage"
        ),
    }
}
