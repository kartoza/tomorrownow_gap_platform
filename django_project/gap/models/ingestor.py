# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

import tempfile

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import models
from django.utils import timezone

from gap.models.dataset import DataSourceFile

User = get_user_model()


def ingestor_file_path(instance, filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}ingestors/{filename}'


class IngestorType:
    """Ingestor type."""

    TAHMO = 'Tahmo'
    FARM = 'Farm'
    CBAM = 'CBAM'
    SALIENT = 'Salient'
    TOMORROWIO = 'Tomorrow.io'
    ARABLE = 'Arable'
    GRID = 'Grid'
    TAHMO_API = 'Tahmo API'
    TIO_FORECAST_COLLECTOR = 'Tio Forecast Collector'
    WIND_BORNE_SYSTEMS_API = 'WindBorne Systems API'
    CABI_PRISE_EXCEL = 'Cabi Prise Excel'
    CBAM_BIAS_ADJUST = 'CBAM Bias Adjusted'


class IngestorSessionStatus:
    """Ingestor status."""

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class BaseSession(models.Model):
    """Base class for Collector and Ingestor Session."""

    class Meta:  # noqa: D106
        abstract = True
        ordering = ['-run_at']

    ingestor_type = models.CharField(
        default=IngestorType.TAHMO,
        choices=(
            (IngestorType.TAHMO, IngestorType.TAHMO),
            (IngestorType.FARM, IngestorType.FARM),
            (IngestorType.CBAM, IngestorType.CBAM),
            (IngestorType.SALIENT, IngestorType.SALIENT),
            (IngestorType.TOMORROWIO, IngestorType.TOMORROWIO),
            (IngestorType.ARABLE, IngestorType.ARABLE),
            (IngestorType.GRID, IngestorType.GRID),
            (IngestorType.TAHMO_API, IngestorType.TAHMO_API),
            (
                IngestorType.TIO_FORECAST_COLLECTOR,
                IngestorType.TIO_FORECAST_COLLECTOR
            ),
            (
                IngestorType.WIND_BORNE_SYSTEMS_API,
                IngestorType.WIND_BORNE_SYSTEMS_API
            ),
            (
                IngestorType.CABI_PRISE_EXCEL,
                IngestorType.CABI_PRISE_EXCEL
            ),
            (IngestorType.CBAM_BIAS_ADJUST, IngestorType.CBAM_BIAS_ADJUST),
        ),
        max_length=512
    )
    status = models.CharField(
        default=IngestorSessionStatus.PENDING,
        choices=(
            (IngestorSessionStatus.PENDING, IngestorSessionStatus.PENDING),
            (IngestorSessionStatus.RUNNING, IngestorSessionStatus.RUNNING),
            (IngestorSessionStatus.SUCCESS, IngestorSessionStatus.SUCCESS),
            (IngestorSessionStatus.FAILED, IngestorSessionStatus.FAILED),
            (
                IngestorSessionStatus.CANCELLED,
                IngestorSessionStatus.CANCELLED
            ),
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
    additional_config = models.JSONField(blank=True, default=dict, null=True)
    is_cancelled = models.BooleanField(default=False)

    def _pre_run(self):
        self.status = IngestorSessionStatus.RUNNING
        self.run_at = timezone.now()
        self.notes = None
        self.end_at = None
        self.save()


class CollectorSession(BaseSession):
    """Class representing data collection session."""

    dataset_files = models.ManyToManyField(DataSourceFile, blank=True)

    def _run(self, working_dir):
        """Run the collector session."""
        from gap.ingestor.cbam import CBAMCollector
        from gap.ingestor.salient import SalientCollector
        from gap.ingestor.tio_shortterm import TioShortTermCollector
        from gap.ingestor.cbam_bias_adjust import CBAMBiasAdjustCollector

        ingestor = None
        if self.ingestor_type == IngestorType.CBAM:
            ingestor = CBAMCollector(self, working_dir)
        elif self.ingestor_type == IngestorType.SALIENT:
            ingestor = SalientCollector(self, working_dir)
        elif self.ingestor_type == IngestorType.TIO_FORECAST_COLLECTOR:
            ingestor = TioShortTermCollector(self, working_dir)
        elif self.ingestor_type == IngestorType.CBAM_BIAS_ADJUST:
            ingestor = CBAMBiasAdjustCollector(self, working_dir)

        if ingestor:
            ingestor.run()

    def run(self):
        """Run the collector session."""
        try:
            self._pre_run()
            with tempfile.TemporaryDirectory() as working_dir:
                self._run(working_dir)
                self.status = (
                    IngestorSessionStatus.SUCCESS if
                    not self.is_cancelled else
                    IngestorSessionStatus.CANCELLED
                )
        except Exception as e:
            self.status = IngestorSessionStatus.FAILED
            self.notes = f'{e}'

        self.end_at = timezone.now()
        self.save()

    def __str__(self) -> str:
        return f'{self.id}-{self.ingestor_type}-{self.status}'


class IngestorSession(BaseSession):
    """Ingestor Session model.

    Attributes:
        ingestor_type (str): Ingestor type.
    """

    file = models.FileField(
        upload_to=ingestor_file_path,
        null=True, blank=True
    )

    collectors = models.ManyToManyField(CollectorSession, blank=True)

    def __init__(self, *args, trigger_task=True, **kwargs):
        """Initialize IngestorSession class."""
        super().__init__(*args, **kwargs)
        # Set the temporary attribute
        self._trigger_task = trigger_task

    def save(self, *args, **kwargs):
        """Override ingestor save."""
        from gap.tasks import run_ingestor_session  # noqa
        created = self.pk is None
        super(IngestorSession, self).save(*args, **kwargs)
        if created and self._trigger_task:
            run_ingestor_session.delay(self.id)

    def _run(self, working_dir):
        """Run the ingestor session."""
        from gap.ingestor.tahmo import TahmoIngestor
        from gap.ingestor.farm import FarmIngestor
        from gap.ingestor.cbam import CBAMIngestor
        from gap.ingestor.salient import SalientIngestor
        from gap.ingestor.grid import GridIngestor
        from gap.ingestor.arable import ArableIngestor
        from gap.ingestor.tahmo_api import TahmoAPIIngestor
        from gap.ingestor.wind_borne_systems import WindBorneSystemsIngestor
        from gap.ingestor.tio_shortterm import TioShortTermIngestor
        from gap.ingestor.cabi_prise import CabiPriseIngestor
        from gap.ingestor.cbam_bias_adjust import CBAMBiasAdjustIngestor

        ingestor = None
        if self.ingestor_type == IngestorType.TAHMO:
            ingestor = TahmoIngestor
        elif self.ingestor_type == IngestorType.FARM:
            ingestor = FarmIngestor
        elif self.ingestor_type == IngestorType.CBAM:
            ingestor = CBAMIngestor
        elif self.ingestor_type == IngestorType.SALIENT:
            ingestor = SalientIngestor
        elif self.ingestor_type == IngestorType.ARABLE:
            ingestor = ArableIngestor
        elif self.ingestor_type == IngestorType.GRID:
            ingestor = GridIngestor
        elif self.ingestor_type == IngestorType.TAHMO_API:
            ingestor = TahmoAPIIngestor
        elif self.ingestor_type == IngestorType.WIND_BORNE_SYSTEMS_API:
            ingestor = WindBorneSystemsIngestor
        elif self.ingestor_type == IngestorType.TOMORROWIO:
            ingestor = TioShortTermIngestor
        elif self.ingestor_type == IngestorType.CABI_PRISE_EXCEL:
            ingestor = CabiPriseIngestor
        elif self.ingestor_type == IngestorType.CBAM_BIAS_ADJUST:
            ingestor = CBAMBiasAdjustIngestor

        if ingestor:
            ingestor(self, working_dir).run()
        else:
            raise Exception(
                f'No Ingestor class for {self.ingestor_type}'
            )

    def run(self):
        """Run the ingestor session."""
        self._pre_run()
        try:
            with tempfile.TemporaryDirectory() as working_dir:
                self._run(working_dir)
                self.status = (
                    IngestorSessionStatus.SUCCESS if
                    not self.is_cancelled else
                    IngestorSessionStatus.CANCELLED
                )
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
