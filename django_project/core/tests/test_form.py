# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for BackgroundTask Model.
"""

from django.test import TestCase
from knox.models import AuthToken

from core.admin import CreateAuthTokenAdmin
from core.factories import UserF
from core.forms import CreateKnoxTokenForm, CreateAuthToken


class TestCreateKnoxTokenForm(TestCase):
    """Create knox token form."""

    def setUp(self):
        """Init test class."""
        self.user_1 = UserF.create(
            is_active=True
        )

    def test_create_knox_token_form(self):
        """Test create knox token form."""
        form = CreateKnoxTokenForm({
            'user': self.user_1
        })
        self.assertTrue(form.is_valid())
        form.save()
        self.assertEqual(AuthToken.objects.count(), 1)

    def test_create_knox_token_admin(self):
        """Test create knox token admin."""
        admin = CreateAuthTokenAdmin(
            model=CreateAuthToken(), admin_site=CreateAuthToken()
        )
        self.assertFalse(admin.has_change_permission(None))
        self.assertFalse(admin.has_delete_permission(None))
        self.assertFalse(admin.has_view_permission(None))
