# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for Swagger utils.
"""

from django.urls import reverse
from django.test import Client

from gap_api.urls.v1 import schema_view_v1
from core.tests.common import BaseAPIViewTest, FakeResolverMatchV1


class TestSwaggerAPI(BaseAPIViewTest):
    """Test swagger API class."""

    def _get_request(self, format=None):
        """Get request to url."""
        request_params = '' if format is None else f'?format={format}'
        request = self.factory.get(
            reverse('api:v1:schema-swagger') + request_params
        )
        request.user = self.user_1
        request.resolver_match = FakeResolverMatchV1
        return request

    def test_swagger_openapiformat(self):
        """Test swagger with openapi format."""
        view = schema_view_v1.with_ui('tomorrownow', cache_timeout=0)
        request = self._get_request(format='openapi')
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn('Weather & Climate Data', str(response.data))

    def test_swagger_ui(self):
        """Test swagger with openapi format."""
        c = Client()
        response = c.get('/api/v1/docs/')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.has_header('Content-Type'))
        self.assertIn('text/html', response.headers['Content-Type'])
        self.assertIn('product_type_list', response.context)
        self.assertIn('attribute_dict', response.context)
