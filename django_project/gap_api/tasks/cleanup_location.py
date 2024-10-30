# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Tasks for Cleanup Expired Location
"""

from celery import shared_task
from django.utils import timezone

from gap_api.models import Location


@shared_task(name="cleanup_user_locations")
def cleanup_user_locations():
    """Cleanup Expired Location."""
    Location.objects.filter(
        expired_on__lte=timezone.now()
    ).delete()
