# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Custom schema.
"""

from django.conf import settings
from drf_yasg.generators import OpenAPISchemaGenerator


class CustomSchemaGenerator(OpenAPISchemaGenerator):
    """Custom schema generator for Open API."""

    def get_schema(self, request=None, public=False):
        """Override list of schema in the API documentation.

        :param request: the request used for filtering accessible endpoints
            and finding the spec URI
        :type request: rest_framework.request.Request or None
        :param bool public: if True, all endpoints are included regardless of
            access through `request`

        :return: the generated Swagger specification
        :rtype: openapi.Swagger
        """
        schema = super().get_schema(request, public)
        schema.schemes = ['https']
        if settings.DEBUG:
            schema.schemes = ['http']
        return schema
