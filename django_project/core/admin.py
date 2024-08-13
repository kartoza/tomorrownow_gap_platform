"""
Tomorrow Now GAP.

.. note:: Definition admin
"""
from django import forms
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import GroupAdmin, UserAdmin
from django.contrib.auth.models import Group

from core.email_receiver import crop_plan_receiver

User = get_user_model()


class AbstractDefinitionAdmin(admin.ModelAdmin):
    """Abstract admin for definition."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)


class GroupAdminForm(forms.ModelForm):
    """ModelForm that contains users of the group.
    """
    users = forms.ModelMultipleChoiceField(
        User.objects.all(),
        widget=admin.widgets.FilteredSelectMultiple('Users', False),
        required=False,
    )

    def __init__(self, *args, **kwargs):
        """Initialise the form."""
        super(GroupAdminForm, self).__init__(*args, **kwargs)
        if self.instance.pk:
            initial_users = self.instance.user_set.values_list('pk', flat=True)
            self.initial['users'] = initial_users

    def save(self, *args, **kwargs):
        """Save the group."""
        kwargs['commit'] = True
        return super(GroupAdminForm, self).save(*args, **kwargs)

    def save_m2m(self):
        """Save the users in the group."""
        self.instance.user_set.clear()
        self.instance.user_set.add(*self.cleaned_data['users'])


admin.site.unregister(Group)


@admin.register(Group)
class CustomGroupAdmin(GroupAdmin):
    """Custom group admin that using GroupAdminForm."""

    form = GroupAdminForm


admin.site.unregister(User)


@admin.register(User)
class CustomUserAdmin(UserAdmin):
    """Custom user admin that using GroupAdminForm."""

    list_display = (
        "username", "email", "first_name", "last_name", "is_staff",
        "receive_email_for_crop_plan"
    )

    def receive_email_for_crop_plan(self, obj):
        """Return if user receive email for crop plan.'"""
        return obj.pk in crop_plan_receiver().values_list(
            'pk', flat=True
        )

    receive_email_for_crop_plan.boolean = True
