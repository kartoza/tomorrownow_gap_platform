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
from gap.models import (
    Farm, Crop, FarmRegistry, FarmRegistryGroup,
    IngestorSession,
)
from django.db import transaction

logger = logging.getLogger(__name__)


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

    def _process_row(self, row):
        """Process a single row from the input file."""
        try:
            # Parse latitude and longitude to create a geometry point
            latitude = float(row['FinalLatitude'])
            longitude = float(row['FinalLongitude'])
            point = Point(x=longitude, y=latitude, srid=4326)

            # Get or create the Farm instance
            farm, _ = Farm.objects.get_or_create(
                unique_id=row['FarmerId'],
                defaults={
                    'geometry': point
                }
            )

            # Get the Crop instance
            crop, _ = Crop.objects.get_or_create(
                name=row['CropName']
            )

            # Parse the planting date
            planting_date = datetime.strptime(
                row['PlantingDate'], '%m/%d/%Y').date()

            # Create the FarmRegistry entry
            FarmRegistry.objects.update_or_create(
                group=self.group,
                farm=farm,
                crop=crop,
                planting_date=planting_date,
            )

        except Exception as e:
            logger.error(f"Error processing row: {row} - {e}")

    def _run(self, dir_path):
        """Run the ingestion logic."""
        self._create_registry_group()
        logger.info(f"Created new registry group: {self.group.id}")

        for file_name in os.listdir(dir_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(dir_path, file_name)
                with open(file_path, 'r') as file:
                    reader = csv.DictReader(file)
                    with transaction.atomic():
                        for row in reader:
                            self._process_row(row)
                break
        else:
            raise FileNotFoundError("No CSV file found in the extracted ZIP.")

    def run(self):
        """Run the ingestion process."""
        if not self.session.file:
            raise FileNotFoundError("No file found in the session.")

        dir_path = self._extract_zip_file()
        try:
            self._run(dir_path)
        finally:
            shutil.rmtree(dir_path)
