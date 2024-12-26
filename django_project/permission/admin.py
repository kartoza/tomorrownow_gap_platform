"""
Tomorrow Now GAP.

.. note:: Admin for permission
"""
from django.contrib import admin
from django.contrib.auth.models import Permission

from permission.models import (
    DatasetTypeGroupObjectPermission,
    DatasetTypeUserObjectPermission,
    PermissionType
)


def _view_dataset_type_permission():
    """Fetch permission to view dataset_type."""
    return Permission.objects.filter(
        codename=PermissionType.VIEW_DATASET_TYPE
    ).first()


@admin.register(DatasetTypeGroupObjectPermission)
class DatasetTypeGroupObjectPermissionAdmin(admin.ModelAdmin):
    """DatasetTypeGroupObjectPermission admin."""

    list_display = ('group', 'content_object',)

    def get_form(self, request, obj=None, **kwargs):
        """Override the permission dropdown."""
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['permission'].disabled = True
        form.base_fields['permission'].initial = (
            _view_dataset_type_permission()
        )
        return form


@admin.register(DatasetTypeUserObjectPermission)
class DatasetTypeUserObjectPermissionAdmin(admin.ModelAdmin):
    """DatasetTypeUserObjectPermission admin."""

    list_display = ('user', 'content_object',)

    def get_form(self, request, obj=None, **kwargs):
        """Override the permission dropdown."""
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['permission'].disabled = True
        form.base_fields['permission'].initial = (
            _view_dataset_type_permission()
        )
        return form
