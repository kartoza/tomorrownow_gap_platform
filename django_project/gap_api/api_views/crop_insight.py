# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from gap_api.serializers.common import APIErrorSerializer
from gap_api.utils.helper import ApiTag


class CropPlanAPI(APIView):
    """API class for crop plan data."""

    permission_classes = [IsAuthenticated]
    api_parameters = [
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
        ],
        responses={
            200: openapi.Schema(
                description=(
                        'Measurement data'
                ),
                type=openapi.TYPE_OBJECT,
                properties={}
            ),
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """Fetch measurement data by attributes and date range filter."""
        return Response(
            status=200,
            data=self.get_response_data()
        )
