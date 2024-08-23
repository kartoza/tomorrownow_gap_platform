# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""
from datetime import datetime

from django.db.utils import ProgrammingError
from django.http import HttpResponseBadRequest
from django.utils import timezone
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.views import APIView

from gap.models import Farm
from gap.models.crop_insight import CropPlanData
from gap_api.renderers import GEOJSONRenderer, CSVDynamicHeaderRenderer
from gap_api.serializers.common import APIErrorSerializer
from gap_api.serializers.crop_insight import (
    CropInsightSerializer, CropInsightGeojsonSerializer
)
from gap_api.utils.helper import ApiTag


def default_fields():
    """Return default fields."""
    try:
        return CropPlanData.default_fields()
    except ProgrammingError:
        return []


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
        GEOJSONRenderer,
        CSVDynamicHeaderRenderer
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
                        "Don't put parameter to return all farm."
                ),
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'generated_date',
                openapi.IN_QUERY,
                description='Default to today or YYYY-MM-DD',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'format', openapi.IN_QUERY,
                description='Output Format',
                type=openapi.TYPE_STRING,
                enum=outputs,
                default=outputs[0]
            ),
            openapi.Parameter(
                'forecast_fields', openapi.IN_QUERY,
                description=(
                        'Forecast fields, use separator. '
                        "Don't put parameter to return all Forecast fields. \n"
                        f"The fields : {default_fields()}"
                ),
                type=openapi.TYPE_STRING
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

        # Generated date
        try:
            generated_date = datetime.strptime(
                request.GET.get(
                    'generated_date', timezone.now().strftime("%Y-%m-%d")
                ),
                "%Y-%m-%d"
            ).date()
        except ValueError:
            return HttpResponseBadRequest(
                'generated_date format is not valid, use YYYY-MM-DD'
            )

        # Get forecast fields
        forecast_fields = request.GET.get('forecast_fields', None)
        if forecast_fields:
            forecast_fields = forecast_fields.split(',')

        # Farms query
        farm_ids = request.GET.get('farm_ids', None)
        if farm_ids:
            farms = Farm.objects.filter(unique_id__in=farm_ids.split(','))
        else:
            farms = Farm.objects.all()

        data = serializer(
            farms, many=True,
            generated_date=generated_date,
            forecast_fields=forecast_fields
        ).data

        return Response(data=data)
