# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Resource classes for Models
"""

from import_export.resources import ModelResource
from gap_api.models import APIRequestLog


class APIRequestLogResource(ModelResource):
    """Resource class for APIRequestLog."""

    class Meta:
        model = APIRequestLog
        fields = [
            "id", "user__username", "query_params", "requested_at",
            "response_ms", "status_code", "view_method", "path",
            "remote_addr", "host"
        ]
        export_order = fields
