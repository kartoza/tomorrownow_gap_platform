# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GAP API urls.
"""

from django.urls import include, re_path

urlpatterns = [
    re_path(
        r'^v1/', include(('gap_api.urls.v1', 'v1'), namespace='v1')
    )
]
