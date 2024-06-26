# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: API helper class.
"""

from drf_yasg import openapi


# API TAGS
USER_API_TAG = '01-user'

# COMMON MANUAL PARAMETERS
PARAMS_PAGINATION = [
    openapi.Parameter(
        'page', openapi.IN_QUERY,
        description='Page number in pagination',
        type=openapi.TYPE_INTEGER,
        default=1
    ), openapi.Parameter(
        'page_size', openapi.IN_QUERY,
        description='Total records in a page',
        type=openapi.TYPE_INTEGER,
        minimum=1,
        maximum=50,
        default=50
    )
]
