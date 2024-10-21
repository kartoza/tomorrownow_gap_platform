"""
Tomorrow Now GAP.

.. note:: Preferences Redirect View
"""
from django.views.generic.base import RedirectView

from gap.models.preferences import Preferences


class PreferencesRedirectView(RedirectView):
    """Redirect to preferences admin page."""

    permanent = False

    def get_redirect_url(self, *args, **kwargs):
        """Return absolute URL to redirect to."""
        Preferences.load()
        return '/admin/gap/preferences/1/change/'
