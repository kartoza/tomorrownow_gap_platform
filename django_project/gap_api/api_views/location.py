# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Location APIs
"""

from django.utils import timezone
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from gap_api.models.location import Location
from gap_api.serializers.common import APIErrorSerializer
from gap_api.serializers.location import LocationSerializer
from gap_api.utils.helper import ApiTag
from gap_api.mixins import GAPAPILoggingMixin


class LocationAPI(GAPAPILoggingMixin, APIView):
    """API class for uploading location."""

    permission_classes = [IsAuthenticated]
    api_parameters = [
        openapi.Parameter(
            'location_name', openapi.IN_QUERY,
            required=True,
            description='Location Name',
            type=openapi.TYPE_STRING
        ),
    ]

    @swagger_auto_schema(
        operation_id='upload-location',
        operation_description=(
            "Upload polygon/points for querying weather data."
        ),
        tags=[ApiTag.Location],
        manual_parameters=[
            *api_parameters
        ],
        responses={
            200: LocationSerializer,
            400: APIErrorSerializer
        }
    )
    def post(self, request, *args, **kwargs):
        """Post method for uploading location."""
        name = request.GET.get('location_name', None)
        if not name:
            raise ValidationError({
                'Invalid Request Parameter': (
                    'location_name is mandatory!'
                )
            })

        # validate uploaded file

        # read geometry

        # create location object
        location = Location.objects.update_or_create(
            user=request.user,
            name=name,
            defaults={
                'created_on': timezone.now(),
                'geometry': None
            }
        )

        return Response(
            status=200, data=LocationSerializer(location).data
        )
