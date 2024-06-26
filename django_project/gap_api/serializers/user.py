# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User serializer class.
"""

from django.contrib.auth import get_user_model
from rest_framework import serializers
from drf_yasg import openapi


User = get_user_model()


class UserInfoSerializer(serializers.ModelSerializer):
    """Serializer for User Info."""

    class Meta:  # noqa
        model = User
        fields = ['username', 'email', 'first_name', 'last_name']
        swagger_schema_fields = {
            'type': openapi.TYPE_OBJECT,
            'title': 'User Info',
            'properties': {
                'username': openapi.Schema(
                    title='Username',
                    type=openapi.TYPE_STRING
                ),
                'email': openapi.Schema(
                    title='User email',
                    type=openapi.TYPE_STRING
                ),
                'first_name': openapi.Schema(
                    title='First Name',
                    type=openapi.TYPE_STRING
                ),
                'last_name': openapi.Schema(
                    title='Last Name',
                    type=openapi.TYPE_STRING
                ),
            },
            'example': {
                'username': 'jane.doe@example.com',
                'email': 'jane.doe@example.com',
                'first_name': 'Jane',
                'last_name': 'Doe'
            }
        }
