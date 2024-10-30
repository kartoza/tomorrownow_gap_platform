# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Common class for unit tests.
"""

from fakeredis import FakeConnection
from django.test import TestCase, override_settings
from rest_framework.test import APIRequestFactory

from core.factories import UserF


@override_settings(
    CACHES={
        'default': {
            'BACKEND': 'django.core.cache.backends.redis.RedisCache',
            'LOCATION': [
                'redis://127.0.0.1:6379',
            ],
            'OPTIONS': {
                'connection_class': FakeConnection
            }
        }
    }
)
class BaseAPIViewTest(TestCase):
    """Base class for API test."""

    def setUp(self):
        """Init test class."""
        self.factory = APIRequestFactory()
        self.superuser = UserF.create(
            is_staff=True,
            is_superuser=True,
            is_active=True
        )
        self.user_1 = UserF.create(
            is_active=True
        )


class FakeResolverMatchV1:
    """Fake class to mock versioning."""

    namespace = 'v1'
