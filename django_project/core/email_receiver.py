"""
Tomorrow Now GAP.

.. note:: Definition admin
"""
from django.contrib.auth.models import Group


def crop_plan_receiver():
    """Return list of user who receive email for crop plan."""
    group, _ = Group.objects.get_or_create(name='Crop Plan Email Receiver')
    return group.user_set.all()
