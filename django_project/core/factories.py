# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Core factories.
"""

from typing import Generic, TypeVar

import factory
from django.contrib.auth import get_user_model

T = TypeVar('T')
User = get_user_model()


class BaseMetaFactory(Generic[T], factory.base.FactoryMetaClass):
    """Base meta factory class."""

    def __call__(cls, *args, **kwargs) -> T:
        """Override the default Factory() syntax to call the default strategy.

        Returns an instance of the associated class.
        """
        return super().__call__(*args, **kwargs)


class BaseFactory(Generic[T], factory.django.DjangoModelFactory):
    """Base factory class to make the factory return correct class typing."""

    @classmethod
    def create(cls, **kwargs) -> T:
        """Create an instance of the model, and save it to the database."""
        return super().create(**kwargs)


class UserF(
    BaseFactory[User], metaclass=BaseMetaFactory[User]
):
    """Factory class for User."""

    class Meta:  # noqa
        model = User

    username = factory.Sequence(
        lambda n: u'username %s' % n
    )
    first_name = 'John'
    last_name = 'Doe'
