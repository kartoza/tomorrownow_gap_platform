"""
Tomorrow Now GAP.

.. note:: Models for permission
"""
from django.db import models
from django.utils.translation import gettext_lazy as _
from guardian.models import UserObjectPermissionBase
from guardian.models import GroupObjectPermissionBase

from gap.models import DatasetType


class PermissionType:
    """Enum that represents the permission type."""

    VIEW_DATASET_TYPE = 'view_datasettype'


class DatasetTypeUserObjectPermission(UserObjectPermissionBase):
    """Model for storing DatasetType Object Permission by user."""

    content_object = models.ForeignKey(DatasetType, on_delete=models.CASCADE)

    class Meta(UserObjectPermissionBase.Meta):  # noqa
        db_table = 'permission_dataset_type_user'
        verbose_name = _('User Product Type Permission')


class DatasetTypeGroupObjectPermission(GroupObjectPermissionBase):
    """Model for storing DatasetType Object Permission by group."""

    content_object = models.ForeignKey(DatasetType, on_delete=models.CASCADE)

    class Meta(GroupObjectPermissionBase.Meta):  # noqa
        db_table = 'permission_dataset_type_group'
        verbose_name = _('Group Product Type Permission')
