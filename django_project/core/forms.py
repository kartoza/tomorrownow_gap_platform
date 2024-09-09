# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Knox auth token form.
"""
from django import forms
from django.contrib import messages
from django.contrib.auth import get_user_model
from knox.models import AuthToken

User = get_user_model()


class CreateAuthToken(AuthToken):
    """Create auth token admin."""

    class Meta:
        app_label = 'knox'
        proxy = True


class CreateKnoxTokenForm(forms.ModelForm):
    """Create knox token of user."""

    user = forms.ModelChoiceField(
        queryset=User.objects.all()
    )

    class Meta:  # noqa: D106
        model = CreateAuthToken
        fields = ['user']

    def save(self, commit=True):
        """Save the instance."""
        instance = super(CreateKnoxTokenForm, self).save(commit=False)
        obj, token = AuthToken.objects.create(
            user=instance.user
        )
        messages.add_message(
            self.request, messages.SUCCESS,
            f'The new token has been generated, please copy : {token}'
        )
        return obj
