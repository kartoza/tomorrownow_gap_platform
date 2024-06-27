# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: User APIs
"""

from drf_yasg.utils import swagger_auto_schema
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from gap_api.serializers.common import APIErrorSerializer
from gap_api.serializers.user import UserInfoSerializer
from gap_api.utils.helper import ApiTag


class UserInfo(APIView):
    """API to return user info."""

    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        operation_id='user-info',
        tags=[ApiTag.USER],
        responses={
            200: UserInfoSerializer,
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """GET method to fetch user info.

        :param request: API Request
        :type request: rest_framework.request.Request
        :return: API response
        :rtype: rest_framework.response.Response
        """
        return Response(
            status=200, data=UserInfoSerializer(request.user).data
        )
