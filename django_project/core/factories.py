# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Core factories.
"""

import factory
from django.contrib.auth import get_user_model

from factory.django import DjangoModelFactory

User = get_user_model()


class UserF(DjangoModelFactory):
    """Factory class for User."""

    class Meta:  # noqa
        model = User

    username = factory.Sequence(
        lambda n: u'username %s' % n
    )
    first_name = 'John'
    last_name = 'Doe'
