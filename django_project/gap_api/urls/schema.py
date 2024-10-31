# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Custom schema.
"""

from collections import OrderedDict
from django.conf import settings
from drf_yasg.generators import OpenAPISchemaGenerator

from gap_api.utils.helper import ApiTag


class CustomSchemaGenerator(OpenAPISchemaGenerator):
    """Custom schema generator for Open API."""

    API_METHODS = ['get', 'post', 'put', 'delete']

    def find_api_method(self, pathItem):
        """Find api method from openapi.PathItem."""
        for method in self.API_METHODS:
            pathMethod = pathItem.get(method, None)
            if pathMethod is not None:
                return method, pathMethod
        return None, None

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

    def get_paths_object(self, paths):
        """Construct the Swagger Paths object.

        :param OrderedDict[str,openapi.PathItem] paths: mapping of paths
            to :class:`.PathItem` objects
        :returns: the :class:`.Paths` object
        :rtype: openapi.Paths
        """
        tag_dict = OrderedDict()
        for tag in ApiTag.ORDERS:
            tag_dict[tag] = []

        for api_path, pathItem in paths.items():
            method, pathMethod = self.find_api_method(pathItem)
            if method is None:
                continue

            tags = pathMethod.get('tags', [])
            tag = tags[0] if len(tags) > 0 else 'other-api'
            if tag in tag_dict:
                tag_dict[tag].append({
                    'path': api_path,
                    'pathItem': pathItem
                })
            else:
                tag_dict[tag] = [{
                    'path': api_path,
                    'pathItem': pathItem
                }]

        results = OrderedDict()
        for tag in tag_dict.keys():
            for api_item in tag_dict[tag]:
                results[api_item['path']] = api_item['pathItem']
        return super().get_paths_object(results)
