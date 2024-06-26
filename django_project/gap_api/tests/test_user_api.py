# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

from django.urls import reverse
from core.tests.common import FakeResolverMatchV1, BaseAPIViewTest
from gap_api.api_views.user import UserInfo


class UserInfoAPITest(BaseAPIViewTest):
    """User info api test case."""

    def test_get_user_info_without_auth(self):
        """Test get user info without authentication."""
        view = UserInfo.as_view()
        request = self.factory.get(
            reverse('v1:user-info')
        )
        request.resolver_match = FakeResolverMatchV1
        response = view(request)
        self.assertEqual(response.status_code, 401)

    def test_get_user_info(self):
        """Test get user info with superuser."""
        view = UserInfo.as_view()
        request = self.factory.get(
            reverse('v1:user-info')
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['username'], self.superuser.username)
        self.assertEqual(
            response.data['first_name'], self.superuser.first_name)
        self.assertEqual(response.data['last_name'], self.superuser.last_name)
