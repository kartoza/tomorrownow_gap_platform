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

    BATCH_SIZE = 100000

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

        self.farm_list = []
        self.registry_list = []


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
        group_name = "farm_registry_" + \
            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
        self.group = self.group_model.objects.create(
            name=group_name,
            date_time=datetime.now(timezone.utc),
            is_latest=True
        )

    def _parse_planting_date(self, date_str):
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

    def _bulk_insert_farms_and_registries(self):
        """Bulk insert Farms first, then FarmRegistry."""
        if not self.farm_list:
            return

        try:
            with transaction.atomic():  # Ensures independent transaction
                # Step 1: Bulk insert farms
                Farm.objects.bulk_create(
                    [Farm(**data) for data in self.farm_list],
                    ignore_conflicts=True
                )

                # Step 2: Retrieve all inserted farms and map them
                farm_map = {
                    farm.unique_id: farm
                    for farm in Farm.objects.filter(
                        unique_id__in=[
                            data["unique_id"] for data in self.farm_list
                        ]
                    )
                }

                # Step 3: Prepare FarmRegistry objects with mapped farms
                registries = []
                for data in self.registry_list:
                    farm_instance = farm_map.get(data["farm_unique_id"])
                    if farm_instance:
                        registries.append(FarmRegistry(
                            group=data["group"],
                            farm=farm_instance,
                            crop=data["crop"],
                            crop_stage_type=data["crop_stage_type"],
                            planting_date=data["planting_date"],
                        ))

                # Step 4: Bulk insert FarmRegistry only if valid records exist
                if registries:
                    FarmRegistry.objects.bulk_create(
                        registries, ignore_conflicts=True)

                # Clear batch lists
                self.farm_list.clear()
                self.registry_list.clear()

        except Exception as e:
            logger.error(f"Bulk insert failed due to {e}")

    def _process_row(self, row):
        """Process a single row from the CSV file."""
        try:
            latitude = float(row[Keys.FINAL_LATITUDE])
            longitude = float(row[Keys.FINAL_LONGITUDE])
            point = Point(x=longitude, y=latitude, srid=4326)

            crop_key = Keys.get_crop_key(row)
            crop_name = row[crop_key].lower().split('_')[0]

            crop = self.crop_lookup.get(crop_name)
            if not crop:
                crop, _ = Crop.objects.get_or_create(name__iexact=crop_name)
                self.crop_lookup[crop_name] = crop

            stage_type = self.stage_lookup.get(row[crop_key])
            if not stage_type:
                stage_type = CropStageType.objects.filter(
                    Q(name__iexact=row[crop_key]) | Q(
                        alias__iexact=row[crop_key])
                ).first()
                self.stage_lookup[row[crop_key]] = stage_type

            farmer_id_key = Keys.get_farm_id_key(row)
            # Store farm data as a dictionary
            farm_data = {
                "unique_id": row[farmer_id_key].strip(),
                "geometry": point,
                "crop": crop
            }
            self.farm_list.append(farm_data)

            planting_date = self._parse_planting_date(
                row[Keys.get_planting_date_key(row)]
            )

            registry_data = {
                "group": self.group,
                "farm_unique_id": row[farmer_id_key].strip(),
                "crop": crop,
                "crop_stage_type": stage_type,
                "planting_date": planting_date,
            }
            self.registry_list.append(registry_data)

            # Batch process every BATCH_SIZE records
            if len(self.farm_list) >= self.BATCH_SIZE:
                self._bulk_insert_farms_and_registries()

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
        # Preload crop & stage types
        self.crop_lookup = {
            c.name.lower(): c for c in Crop.objects.all()
        }
        self.stage_lookup = {
            s.name.lower(): s for s in CropStageType.objects.all()
        }

        dir_path = self._extract_zip_file()
        try:
            self._run(dir_path)
        finally:
            shutil.rmtree(dir_path)

        # Final batch insert if records are left
        if len(self.farm_list) >= self.BATCH_SIZE:
            self._bulk_insert_farms_and_registries()
