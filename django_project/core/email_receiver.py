"""
Tomorrow Now GAP.

.. note:: Definition admin
"""
from django.contrib.auth.models import Group


def crop_plan_receiver_group():
    """Return group who receive email for crop plan."""
    group, _ = Group.objects.get_or_create(name='Crop Plan Email Receiver')
    return group


def crop_plan_receiver():
    """Return list of user who receive email for crop plan."""
    return crop_plan_receiver_group().user_set.all()
