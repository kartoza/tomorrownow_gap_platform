# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Project level settings.
"""
import os  # noqa
from boto3.s3.transfer import TransferConfig

from .contrib import *  # noqa
from .utils import absolute_path

ALLOWED_HOSTS = ['*']
ADMINS = ()
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.environ['DATABASE_NAME'],
        'USER': os.environ['DATABASE_USERNAME'],
        'PASSWORD': os.environ['DATABASE_PASSWORD'],
        'HOST': os.environ['DATABASE_HOST'],
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
    'gap',
    'gap_api'
)

TEMPLATES[0]['DIRS'] += [
    absolute_path('frontend', 'templates'),
]

# Hyrax Base URL
OPENDAP_BASE_URL = 'http://hyrax:8080/opendap/'
OPENDAP_FILE_CACHE_DIR = '/home/web/opendap'

MB = 1024 ** 2
AWS_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=512 * MB,
    use_threads=True,
    max_concurrency=10
)
