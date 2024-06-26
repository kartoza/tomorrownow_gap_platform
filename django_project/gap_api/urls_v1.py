# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: GAP API v1 urls.
"""


from django.urls import path
from gap_api.api_views.user import UserInfo


# USER API
user_urls = [
    path(
        'user/me',
        UserInfo.as_view(),
        name='user-info'
    ),
]


urlpatterns = []
urlpatterns += user_urls
