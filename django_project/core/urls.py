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
    def get_schema(self, request=None, public=False):
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
