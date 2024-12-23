"""
Tomorrow Now GAP.

.. note:: Group for crop plan email receiver
"""
from django.contrib.auth.models import Group


def _group_crop_plan_receiver():
    """Return group crop plan receiver."""
    group, _ = Group.objects.get_or_create(name='Crop Plan Email Receiver')
    return group


def crop_plan_receiver():
    """Return list of user who receive email for crop plan."""
    return _group_crop_plan_receiver().user_set.all()
