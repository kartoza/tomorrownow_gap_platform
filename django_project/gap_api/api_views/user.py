# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User APIs
"""

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from gap_api.serializers.common import APIErrorSerializer
from gap_api.serializers.user import UserInfoSerializer
from gap_api.utils.api_helper import USER_API_TAG


class UserInfo(APIView):
    """API to return user info."""

    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        operation_id='user-info',
        tags=[USER_API_TAG],
        responses={
            200: UserInfoSerializer,
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        return Response(
            status=200, data=UserInfoSerializer(request.user).data)
