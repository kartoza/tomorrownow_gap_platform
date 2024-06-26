"""Tomorrow Now GAP."""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path
from rest_framework import permissions, authentication
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
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


schema_view_v1 = get_schema_view(
    openapi.Info(
        title="Global Access Platform API",
        default_version='v0.0.1'
    ),
    public=True,
    authentication_classes=[authentication.SessionAuthentication],
    permission_classes=[permissions.AllowAny],
    generator_class=CustomSchemaGenerator,
    patterns=[
        re_path(r'api/v1/', include(
            ('gap_api.urls_v1', 'api'),
            namespace='v1')
        )
    ],
)


urlpatterns = [
    re_path(r'^api/v1/docs/$', schema_view_v1.with_ui(
        'redoc', cache_timeout=0), name='schema-redoc'),
    re_path(r'^api/v1/',
            include(('gap_api.urls_v1', 'api'), namespace='v1')),
    path('admin/', admin.site.urls),
    path('', include('frontend.urls')),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )
