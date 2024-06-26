# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Core factories.
"""

import factory
from typing import Generic, TypeVar
from django.contrib.auth import get_user_model


T = TypeVar('T')
User = get_user_model()


class BaseMetaFactory(Generic[T], factory.base.FactoryMetaClass):
    """Base meta factory class."""

    def __call__(cls, *args, **kwargs) -> T:
        return super().__call__(*args, **kwargs)


class BaseFactory(Generic[T], factory.django.DjangoModelFactory):
    """Base factory class to make the factory return correct class typing."""

    @classmethod
    def create(cls, **kwargs) -> T:
        return super().create(**kwargs)


class UserF(BaseFactory[User],
            metaclass=BaseMetaFactory[User]):
    """Factory class for User."""

    class Meta:  # noqa
        model = User

    username = factory.Sequence(
        lambda n: u'username %s' % n
    )
    first_name = 'John'
    last_name = 'Doe'
