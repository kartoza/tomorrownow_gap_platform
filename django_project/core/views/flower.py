"""
Tomorrow Now GAP.

.. note:: Flower Redirect View
"""
from django.urls import re_path
from django.contrib.auth.mixins import UserPassesTestMixin
from revproxy.views import ProxyView


class FlowerProxyView(UserPassesTestMixin, ProxyView):
    """Redirect request to Flower monitoring tools."""

    upstream = 'http://{}:{}'.format('127.0.0.1', 9090)
    url_prefix = 'flower'
    rewrite = (
        (r'^/{}$'.format(url_prefix), r'/{}/'.format(url_prefix)),
    )

    def test_func(self):
        """Restrict access to superuser."""
        return self.request.user.is_superuser

    @classmethod
    def as_url(cls):
        """Get URLs for flower."""
        return re_path(
            r'^(?P<path>{}.*)$'.format(cls.url_prefix),
            cls.as_view()
        )
