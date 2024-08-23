# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

from django.http import HttpResponseBadRequest
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.views import APIView

from gap.models import Farm
from gap_api.renderers import GEOJSONRenderer
from gap_api.serializers.common import APIErrorSerializer
from gap_api.serializers.crop_insight import (
    CropInsightSerializer, CropInsightGeojsonSerializer
)
from gap_api.utils.helper import ApiTag


class CropPlanAPI(APIView):
    """API class for crop plan data."""

    permission_classes = [IsAuthenticated]
    outputs = [
        'json',
        'geojson',
        'csv'
    ]
    renderer_classes = [
        BrowsableAPIRenderer,
        JSONRenderer,
        GEOJSONRenderer
    ]

    @swagger_auto_schema(
        operation_id='get-crop-plan',
        tags=[ApiTag.CROP_PLAN],
        manual_parameters=[
            openapi.Parameter(
                'farm_ids',
                openapi.IN_QUERY,
                description=(
                        'List of farm id, use comma separator. '
                        'Put "all" for returning all farms.'
                ),
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'generated_date',
                openapi.IN_QUERY,
                description='Start Date',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'format', openapi.IN_QUERY,
                description='Output Format',
                type=openapi.TYPE_STRING,
                enum=outputs,
                default=outputs[0]
            ),
        ],
        responses={
            200: openapi.Schema(
                description='Crop plan data',
                type=openapi.TYPE_OBJECT,
                properties={}
            ),
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """Fetch measurement data by attributes and date range filter."""
        # output format
        output_format = request.GET.get('format', self.outputs[0])

        # Check outputs
        serializer = CropInsightSerializer
        if output_format == 'json':
            serializer = CropInsightSerializer
        elif output_format == 'geojson':
            serializer = CropInsightGeojsonSerializer

        # Farms query
        farms = Farm.objects.all()
        return Response(
            data=serializer(farms, many=True).data
        )
