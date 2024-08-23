# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Crop insight serializer class.
"""

from django.utils import timezone
from rest_framework.serializers import ModelSerializer
from rest_framework_gis.serializers import GeoFeatureModelSerializer

from gap.models.crop_insight import CropPlanData
from gap.models.farm import Farm


class _BaseCropInsightSerializer:
    """Base serializer for crop insight."""

    def __init__(self, *args, **kwargs):
        """Init class."""
        self.generated_date = kwargs.pop('generated_date', timezone.now())
        self.forecast_fields = kwargs.pop('forecast_fields', None)
        super(_BaseCropInsightSerializer, self).__init__(*args, **kwargs)

    def to_representation(self, farm: Farm):
        """To representation."""
        representation = super().to_representation(farm)
        context = CropPlanData(
            farm, self.generated_date,
            forecast_fields=self.forecast_fields
        ).data
        representation.update(context)
        return representation

    class Meta:  # noqa
        model = Farm
        geo_field = 'geometry'
        fields = []


class CropInsightSerializer(
    _BaseCropInsightSerializer, ModelSerializer
):
    """Serializer for crop insight."""

    pass


class CropInsightGeojsonSerializer(
    _BaseCropInsightSerializer, GeoFeatureModelSerializer
):
    """Serializer for crop insight in geojson."""

    pass
