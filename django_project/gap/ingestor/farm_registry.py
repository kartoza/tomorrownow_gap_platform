# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Ingestor for DCAS Farmer Registry data
"""

import csv
import os
import shutil
import uuid
import tempfile
import zipfile
from datetime import datetime, timezone
from django.contrib.gis.geos import Point
import logging
from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException,
)
from gap.models import (
    Farm, Crop, FarmRegistry, FarmRegistryGroup,
    IngestorSession, CropStageType,
    IngestorSessionStatus
)
from django.db import transaction
from django.db.models import Q

logger = logging.getLogger(__name__)


class Keys:
    """Keys for the data."""

    CROP = 'CropName'
    ALT_CROP = 'crop'
    FARMER_ID = 'FarmerId'
    ALT_FARMER_ID = 'farmer_id'
    FINAL_LATITUDE = 'FinalLatitude'
    FINAL_LONGITUDE = 'FinalLongitude'
    PLANTING_DATE = 'PlantingDate'
    ALT_PLANTING_DATE = 'plantingDate'

    @staticmethod
    def check_columns(df) -> bool:
        """Check if all columns exist in dataframe.

        :param df: dataframe from csv
        :type df: pd.DataFrame
        :raises FileIsNotCorrectException: When column is missing
        """
        keys = [
            Keys.CROP, Keys.FARMER_ID, Keys.FINAL_LATITUDE,
            Keys.FINAL_LONGITUDE, Keys.PLANTING_DATE
        ]

        missing = []
        for key in keys:
            if key not in df.columns:
                missing.append(key)

        if missing:
            raise FileIsNotCorrectException(
                f'Column(s) missing: {",".join(missing)}'
            )

    @staticmethod
    def get_crop_key(row):
        """Handle both 'CropName' and 'crop' key variations."""
        if Keys.CROP in row:
            return Keys.CROP
        elif Keys.ALT_CROP in row:
            return Keys.ALT_CROP
        else:
            raise KeyError(f"No valid crop key found in row: {row}")

    @staticmethod
    def get_planting_date_key(row):
        """Handle both 'PlantingDate' and 'plantingDate' key variations."""
        if Keys.PLANTING_DATE in row:
            return Keys.PLANTING_DATE
        elif Keys.ALT_PLANTING_DATE in row:
            return Keys.ALT_PLANTING_DATE
        else:
            raise KeyError(f"No valid planting date key found in row: {row}")

    @staticmethod
    def get_farm_id_key(row):
        """Handle both 'FarmerId' and 'farmer_id' key variations."""
        if Keys.FARMER_ID in row:
            return Keys.FARMER_ID
        elif Keys.ALT_FARMER_ID in row:
            return Keys.ALT_FARMER_ID
        else:
            raise KeyError(f"No valid farmer ID key found in row: {row}")


class DCASFarmRegistryIngestor(BaseIngestor):
    """Ingestor for DCAS Farmer Registry data."""

    def __init__(self, session: IngestorSession, working_dir='/tmp'):
        """Initialize the ingestor with session and working directory.

        :param session: Ingestor session object
        :type session: IngestorSession
        :param working_dir: Directory to extract ZIP files temporarily
        :type working_dir: str, optional
        """
        super().__init__(session, working_dir)

        # Initialize the FarmRegistryGroup model
        self.group_model = FarmRegistryGroup

        # Placeholder for the group created during this session
        self.group = None

    def _extract_zip_file(self):
        """Extract the ZIP file to a temporary directory."""
        dir_path = os.path.join(self.working_dir, str(uuid.uuid4()))
        os.makedirs(dir_path, exist_ok=True)

        with self.session.file.open('rb') as zip_file:
            with tempfile.NamedTemporaryFile(
                delete=False, dir=self.working_dir) as tmp_file:

                tmp_file.write(zip_file.read())
                tmp_file_path = tmp_file.name

        with zipfile.ZipFile(tmp_file_path, 'r') as zip_ref:
            zip_ref.extractall(dir_path)

        os.remove(tmp_file_path)
        return dir_path

    def _create_registry_group(self):
        """Create a new FarmRegistryGroup."""
        self.group = self.group_model.objects.create(
            date_time=datetime.now(timezone.utc),
            is_latest=True
        )

    def parse_planting_date(self, date_str):
        """Try multiple date formats for planting date."""
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y']

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        # If no format worked, log an error
        logger.error(f"Invalid date format: {date_str}")
        return None  # Return None for invalid dates


    def _process_row(self, row):
        """Process a single row from the input file."""
        try:
            # Parse latitude and longitude to create a geometry point
            latitude = float(row[Keys.FINAL_LATITUDE])
            longitude = float(row[Keys.FINAL_LONGITUDE])
            point = Point(x=longitude, y=latitude, srid=4326)

            # get crop and stage type
            crop_key = Keys.get_crop_key(row)
            crop_with_stage = row[crop_key].lower().split('_')
            crop, _ = Crop.objects.get_or_create(
                name__iexact=crop_with_stage[0],
                defaults={
                    'name': crop_with_stage[0].title()
                }
            )
            stage_type = CropStageType.objects.get(
                Q(name__iexact=crop_with_stage[1]) |
                Q(alias__iexact=crop_with_stage[1])
            )

            # Parse planting date dynamically
            planting_date_key = Keys.get_planting_date_key(row)
            planting_date = self.parse_planting_date(row[planting_date_key])

            # Get or create the Farm instance
            farmer_id_key = Keys.get_farm_id_key(row)
            farm, _ = Farm.objects.get_or_create(
                unique_id=row[farmer_id_key].strip(),
                defaults={
                    'geometry': point,
                    'crop': crop
                }
            )

            # Create the FarmRegistry entry
            FarmRegistry.objects.update_or_create(
                group=self.group,
                farm=farm,
                crop=crop,
                crop_stage_type=stage_type,
                planting_date=planting_date,
            )

        except Exception as e:
            logger.error(f"Error processing row: {row} - {e}")

    def _run(self, dir_path):
        """Run the ingestion logic."""
        self._create_registry_group()
        logger.debug(f"Created new registry group: {self.group.id}")
        file_found = False
        has_failures = False

        for file_name in os.listdir(dir_path):
            if file_name.endswith('.csv'):
                file_found = True
                file_path = os.path.join(dir_path, file_name)
                try:
                    with open(file_path, 'r') as file:
                        reader = csv.DictReader(file)
                        with transaction.atomic():
                            for row in reader:
                                try:
                                    self._process_row(row)
                                except KeyError as e:
                                    logger.error(f"{e} in file {file_name}")
                                    has_failures = True
                except Exception as e:
                    logger.error(f"Failed to process {file_name}: {e}")
                    self.session.status = IngestorSessionStatus.FAILED
                    self.session.notes = str(e)
                    self.session.save()
                    return  # Stop execution if we can't process the file

                break

        if not file_found:
            logger.error("No CSV file found in the extracted ZIP.")
            self.session.status = IngestorSessionStatus.FAILED
            self.session.notes = "No CSV file found."
        elif has_failures:
            self.session.status = IngestorSessionStatus.FAILED
            self.session.notes = "Some rows failed to process."
        else:
            self.session.status = IngestorSessionStatus.SUCCESS
            logger.info(
                f"Ingestor session {self.session.id} completed successfully."
            )

        self.session.save()  # Save session **only once**

    def run(self):
        """Run the ingestion process."""
        if not self.session.file:
            raise FileNotFoundException("No file found for ingestion.")
        dir_path = self._extract_zip_file()
        try:
            self._run(dir_path)
        finally:
            shutil.rmtree(dir_path)
