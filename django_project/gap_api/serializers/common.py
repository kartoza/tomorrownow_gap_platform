# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Common serializer class.
"""

from rest_framework import serializers


class APIErrorSerializer(serializers.Serializer):
    """Serializer for error in the API."""

    detail = serializers.CharField()


class NoContentSerializer(serializers.Serializer):
    """Empty serializer for API that returns 204 No Content."""

    pass
