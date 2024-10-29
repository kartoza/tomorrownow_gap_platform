# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Location serializer class.
"""

from rest_framework import serializers

from gap_api.models.location import Location


class LocationSerializer(serializers.ModelSerializer):
    """Serializer for Location."""

    location_name = serializers.CharField(source='name')

    class Meta:  # noqa
        model = Location
        fields = ['location_name', 'created_on', 'expired_on']
        swagger_schema_fields = {
            'title': 'Location Data',
            'example': {
                'location_name': 'location A',
                'created_on': '2024-10-01T00:00:00Z',
                'expired_on': '2024-12-01T00:00:00Z'
            }
        }
