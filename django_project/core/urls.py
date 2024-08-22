"""Tomorrow Now GAP."""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path
from django.views.generic.base import RedirectView

from gap.models.preferences import Preferences


class PreferencesRedirectView(RedirectView):
    """Redirect to preferences admin page."""

    permanent = False

    def get_redirect_url(self, *args, **kwargs):
        """Return absolute URL to redirect to."""
        Preferences.load()
        return '/admin/gap/preferences/1/change/'


urlpatterns = [
    re_path(
        r'^api/', include(('gap_api.urls', 'api'), namespace='api')
    ),
    re_path(
        r'^admin/gap/preferences/$', PreferencesRedirectView.as_view(),
        name='index'
    ),
    path('admin/', admin.site.urls),
    path('', include('frontend.urls')),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )
