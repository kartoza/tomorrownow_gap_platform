# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Project level settings.
"""
import os  # noqa

from .contrib import *  # noqa
from .utils import absolute_path

ALLOWED_HOSTS = ['*']
ADMINS = ()
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': 'django',
        'USER': 'docker',
        'PASSWORD': 'docker',
        'HOST': 'db',
        'PORT': 5432,
        'TEST_NAME': 'unittests',
    }
}

# Set debug to false for production
DEBUG = TEMPLATE_DEBUG = False

# Extra installed apps
INSTALLED_APPS = INSTALLED_APPS + (
    'core',
    'frontend',
)

TEMPLATES[0]['DIRS'] += [
    absolute_path('frontend', 'templates'),
]
