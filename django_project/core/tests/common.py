# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Common class for unit tests.
"""

from django.test import TestCase
from rest_framework.test import APIRequestFactory
from core.factories import UserF


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
