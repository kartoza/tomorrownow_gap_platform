# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import models
from django.utils import timezone

User = get_user_model()


def ingestor_file_path(instance, filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}ingestors/{filename}'


class IngestorType:
    """Ingestor type."""

    TAHMO = 'Tahmo'
    FARM = 'Farm'
    CBAM = 'CBAM'


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
            (IngestorType.FARM, IngestorType.FARM),
            (IngestorType.CBAM, IngestorType.CBAM),
        ),
        max_length=512
    )
    file = models.FileField(
        upload_to=ingestor_file_path,
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
        from gap.ingestor.farm import FarmIngestor
        from gap.ingestor.cbam import CBAMIngestor

        ingestor = None
        if self.ingestor_type == IngestorType.TAHMO:
            ingestor = TahmoIngestor(self)
        elif self.ingestor_type == IngestorType.FARM:
            ingestor = FarmIngestor(self)
        elif self.ingestor_type == IngestorType.CBAM:
            ingestor = CBAMIngestor(self)

        if ingestor:
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


class IngestorSessionProgress(models.Model):
    """Ingestor Session Progress model."""

    session = models.ForeignKey(
        IngestorSession, on_delete=models.CASCADE
    )
    filename = models.TextField()
    status = models.CharField(
        default=IngestorSessionStatus.RUNNING,
        choices=(
            (IngestorSessionStatus.RUNNING, IngestorSessionStatus.RUNNING),
            (IngestorSessionStatus.SUCCESS, IngestorSessionStatus.SUCCESS),
            (IngestorSessionStatus.FAILED, IngestorSessionStatus.FAILED),
        ),
        max_length=512
    )
    row_count = models.IntegerField()
    notes = models.TextField(
        blank=True, null=True
    )
