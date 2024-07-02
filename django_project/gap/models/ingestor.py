# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

import os

from django.contrib.auth import get_user_model
from django.db import models
from django.dispatch import receiver
from django.utils import timezone

User = get_user_model()


class IngestorType:
    """Ingestor type."""

    TAHMO = 'Tahmo'


class IngestorSessionStatus:
    """Ingestor status."""

    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


class IngestorSession(models.Model):
    """Ingestor Session model.

    Attributes:
        ingestor_type (str): Ingestor type.
    """

    ingestor_type = models.CharField(
        default=IngestorType.TAHMO,
        choices=(
            (IngestorType.TAHMO, IngestorType.TAHMO),
        ),
        max_length=512
    )
    file = models.FileField(
        upload_to='ingestors/',
        null=True, blank=True
    )
    status = models.CharField(
        default=IngestorSessionStatus.RUNNING,
        choices=(
            (IngestorSessionStatus.RUNNING, IngestorSessionStatus.RUNNING),
            (IngestorSessionStatus.SUCCESS, IngestorSessionStatus.SUCCESS),
            (IngestorSessionStatus.FAILED, IngestorSessionStatus.FAILED),
        ),
        max_length=512
    )
    notes = models.TextField(
        blank=True, null=True
    )

    run_at = models.DateTimeField(
        auto_now_add=True
    )
    end_at = models.DateTimeField(
        blank=True, null=True
    )

    def save(self, *args, **kwargs):
        """Override ingestor save."""
        from gap.tasks import run_ingestor_session  # noqa
        created = self.pk is None
        super(IngestorSession, self).save(*args, **kwargs)
        if created:
            run_ingestor_session.delay(self.id)

    def _run(self):
        """Run the ingestor session."""
        from gap.ingestor.tahmo import TahmoIngestor
        if self.ingestor_type == IngestorType.TAHMO:
            ingestor = TahmoIngestor(self)
            ingestor.run()

    def run(self):
        """Run the ingestor session."""
        try:
            self._run()
            self.status = IngestorSessionStatus.SUCCESS
        except Exception as e:
            self.status = IngestorSessionStatus.FAILED
            self.notes = f'{e}'

        self.end_at = timezone.now()
        self.save()


@receiver(models.signals.post_delete, sender=IngestorSession)
def auto_delete_file_on_delete(sender, instance, **kwargs):
    """Delete file from filesystem.

    when corresponding `IngestorSession` object is deleted.
    """
    if instance.file:
        if os.path.isfile(instance.file.path):
            os.remove(instance.file.path)


@receiver(models.signals.pre_save, sender=IngestorSession)
def auto_delete_file_on_change(sender, instance, **kwargs):
    """Delete old file from filesystem.

    when corresponding `IngestorSession` object is updated
    with new file.
    """
    if not instance.pk:
        return False

    try:
        old_file = IngestorSession.objects.get(pk=instance.pk).file
    except IngestorSession.DoesNotExist:
        return False

    new_file = instance.file
    if not old_file == new_file:
        try:
            if os.path.isfile(old_file.path):
                os.remove(old_file.path)
        except ValueError:
            pass
