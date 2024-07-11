# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models for SPW R code
"""
from django.db import models
from django.contrib.auth import get_user_model
from django.dispatch import receiver
from django.db.models.signals import post_save, post_delete


User = get_user_model()


class RModel(models.Model):
    """Model that stores R code."""

    name = models.CharField(max_length=256)
    version = models.FloatField()
    code = models.TextField()
    notes = models.TextField(
        null=True,
        blank=True
    )
    created_on = models.DateTimeField()
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    updated_on = models.DateTimeField()
    updated_by = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='rmodel_updater')


class RModelOutputType:
    """R model output type."""

    GO_NO_GO_STATUS = 'goNoGo'
    DAYS_h2TO_F2 = 'days_h2to_f2'
    DAYS_f3TO_F5 = 'days_f3to_f5'
    DAYS_f6TO_F13 = 'days_f6to_f13'
    NEAR_DAYS_LTN_PERCENT = 'nearDaysLTNPercent'
    NEAR_DAYS_CUR_PERCENT = 'nearDaysCurPercent'


class RModelOutput(models.Model):
    """Model that stores relationship between R Model and its outputs."""

    model = models.ForeignKey(RModel, on_delete=models.CASCADE)
    type = models.CharField(
        max_length=100,
        choices=(
            (RModelOutputType.GO_NO_GO_STATUS,
             RModelOutputType.GO_NO_GO_STATUS),
            (RModelOutputType.DAYS_h2TO_F2,
             RModelOutputType.DAYS_h2TO_F2),
            (RModelOutputType.DAYS_f3TO_F5,
             RModelOutputType.DAYS_f3TO_F5),
            (RModelOutputType.DAYS_f6TO_F13,
             RModelOutputType.DAYS_f6TO_F13),
            (RModelOutputType.NEAR_DAYS_LTN_PERCENT,
             RModelOutputType.NEAR_DAYS_LTN_PERCENT),
            (RModelOutputType.NEAR_DAYS_CUR_PERCENT,
             RModelOutputType.NEAR_DAYS_CUR_PERCENT),
        )
    )
    variable_name = models.CharField(max_length=100)


@receiver(post_save, sender=RModel)
def rmodel_post_create(sender, instance: RModel,
                       created, *args, **kwargs):
    """Restart plumber process when a RModel is created."""
    from spw.tasks import (
        start_plumber_process
    )
    if instance.code and instance.id:
        start_plumber_process.apply_async(queue='plumber')


@receiver(post_delete, sender=RModel)
def rmodel_post_delete(sender, instance: RModel,
                       *args, **kwargs):
    """Restart plumber process when a RModel is deleted."""
    from spw.tasks import (
        start_plumber_process
    )
    # respawn Plumber API
    start_plumber_process.apply_async(queue='plumber')
