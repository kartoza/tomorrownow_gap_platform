# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Common serializer class.
"""

from django.contrib.gis.geos import Point
from rest_framework import serializers


class APIErrorSerializer(serializers.Serializer):
    """Serializer for error in the API."""

    detail = serializers.CharField()


class NoContentSerializer(serializers.Serializer):
    """Empty serializer for API that returns 204 No Content."""

    pass


class LatitudeField(serializers.Field):
    """Latitude Field."""

    def to_representation(self, geometry: Point):
        """To represent Latitude Field."""
        return geometry.y


class LongitudeField(serializers.Field):
    """Longitude Field."""

    def to_representation(self, geometry: Point):
        """To represent Longitude Field."""
        return geometry.x
