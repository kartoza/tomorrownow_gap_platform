# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor for DCAS GDD data
"""

import os
import json
import logging
from django.db import transaction
from gap.ingestor.base import BaseIngestor
from gap.models.crop_insight import Crop, CropGrowthStage
from dcas.models import GDDConfig, GDDMatrix
from gap.models import (
    IngestorSession, IngestorSessionProgress, IngestorSessionStatus
)

logger = logging.getLogger(__name__)


class GDDIngestor(BaseIngestor):
    """Ingestor for processing GDD data"""

    def __init__(
        self, session: IngestorSession, fixture_path: str, working_dir='/tmp'):
        """
        Initialize the ingestor.

        :param session: Ingestor session object.
        :type session: IngestorSession
        :param fixture_path: Path to the JSON fixture file.
        :type fixture_path: str
        :param working_dir: Working directory for temporary processing.
        :type working_dir: str
        """
        super().__init__(session, working_dir)
        self.fixture_path = fixture_path

    def _load_fixture(self):
        """Load data from the specified fixture file."""
        with open(self.fixture_path, 'r') as file:
            return json.load(file)

    def _process_fixture_entry(self, entry):
        """Process a single fixture entry."""
        try:
            crop_name = entry['crop']
            crop_growth_stage_name = entry['crop_growth_stage']
            crop_stage_type = entry['crop_stage_type']
            base_temp = entry['base_temp']
            cap_temp = entry['cap_temp']
            gdd_threshold = entry['gdd_threshold']

            # Get or create the Crop instance
            crop, _ = Crop.objects.get_or_create(name=crop_name)

            # Get or create the CropGrowthStage instance
            crop_growth_stage, _ = CropGrowthStage.objects.get_or_create(
                name=crop_growth_stage_name
            )

            # Get or create the GDDConfig
            gdd_config, _ = GDDConfig.objects.get_or_create(
                crop=crop,
                config=self.session.config,
                defaults={
                    'base_temperature': base_temp,
                    'cap_temperature': cap_temp
                }
            )

            # Create the GDDMatrix entry
            GDDMatrix.objects.update_or_create(
                crop=crop,
                crop_growth_stage=crop_growth_stage,
                crop_stage_type=crop_stage_type,
                config=self.session.config,
                defaults={
                    'gdd_threshold': gdd_threshold
                }
            )

        except KeyError as e:
            logger.error(f"Missing key in fixture entry: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing fixture entry: {entry} - {e}")
            raise

    @transaction.atomic
    def _run(self):
        """Run the ingestion logic."""
        fixtures = self._load_fixture()
        progress = IngestorSessionProgress.objects.create(
            session=self.session,
            filename=self.fixture_path,
            row_count=len(fixtures)
        )

        for entry in fixtures:
            try:
                self._process_fixture_entry(entry)
            except Exception as e:
                logger.error(f"Error processing entry: {entry} - {e}")

        progress.status = IngestorSessionStatus.SUCCESS
        progress.save()
        logger.info("GDD data ingested successfully.")

    def run(self):
        """Run the ingestor."""
        if not os.path.exists(self.fixture_path):
            raise FileNotFoundError(
                f"Fixture file not found: {self.fixture_path}")

        self._run()
