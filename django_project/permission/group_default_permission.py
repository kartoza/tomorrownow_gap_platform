"""
Tomorrow Now GAP.

.. note:: Group for default permission
"""
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver


User = get_user_model()


def group_gap_default():
    """Return group GAP Default."""
    group, _ = Group.objects.get_or_create(name='GAP Default')
    return group


@receiver(post_save, sender=User)
def post_save_user_signal_handler(sender, instance, created, **kwargs):
    """Assign gap_default group to new user."""
    if created:
        instance.groups.add(group_gap_default())
