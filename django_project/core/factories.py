# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Core factories.
"""

import factory
from django.contrib.auth import get_user_model
from factory.django import DjangoModelFactory
from django.utils import timezone

from core.models import BackgroundTask


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


class BackgroundTaskF(DjangoModelFactory):
    """Factory class for BackgroundTask."""

    class Meta:  # noqa
        model = BackgroundTask

    task_name = factory.Sequence(
        lambda n: u'task-name %s' % n
    )
    task_id = factory.Faker('uuid4')
    submitted_by = factory.SubFactory(UserF)
