# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm ingestor.
"""

import pandas as pd

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    IngestorSession, Farm, FarmGroup, Crop, FarmCategory, FarmRSVPStatus,
    Village
)
from gap.utils.dms import dms_string_to_point

COLUMN_COUNT = 9
HEADER_IDX = 1


class Keys:
    """Keys for the data."""

    FARM_ID = 'Farm ID'
    PHONE_NUMBER = 'Phone Number'
    VILLAGE_NAME = 'Village Name'
    GEOMETRY = 'Farm Location (Lat/Long)'
    RSVP = 'RSVP status'
    CATEGORY = 'Category'
    CROP = 'Trial Crop'


class FarmIngestor(BaseIngestor):
    """Ingestor for Farm data."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)
        self.farm_group = None
        try:
            self.farm_group = FarmGroup.objects.get(
                id=session.additional_config['farm_group_id']
            )
        except KeyError as e:
            raise AdditionalConfigNotFoundException('farm_group_id')
        except FarmGroup.DoesNotExist:
            raise Exception('Farm group does not exist')

    def _run(self):
        """Run the ingestor."""
        df = pd.read_excel(
            self.session.file.read(), sheet_name=0, header=HEADER_IDX,
            converters={
                Keys.FARM_ID: str,
                Keys.PHONE_NUMBER: str
            }
        )
        df.reset_index(drop=True, inplace=True)
        data = df.to_dict(orient='records')

        # Process the farm
        for idx, row in enumerate(data):
            try:
                farm_id = row[Keys.FARM_ID]
                phone_number = row[Keys.PHONE_NUMBER]
                geometry = dms_string_to_point(row[Keys.GEOMETRY])
                crop, _ = Crop.objects.get_or_create(name=row[Keys.CROP])
                category, _ = FarmCategory.objects.get_or_create(
                    name=row[Keys.CATEGORY]
                )
                rsvp_status, _ = FarmRSVPStatus.objects.get_or_create(
                    name=row[Keys.RSVP]
                )
                village, _ = Village.objects.get_or_create(
                    name=row[Keys.VILLAGE_NAME]
                )
                farm, _ = Farm.objects.update_or_create(
                    unique_id=farm_id,
                    geometry=geometry,
                    defaults={
                        'crop': crop,
                        'category': category,
                        'rsvp_status': rsvp_status,
                        'village': village,
                        'phone_number': phone_number
                    }
                )
                self.farm_group.farms.add(farm)
            except KeyError as e:
                raise FileIsNotCorrectException(
                    f'Row {idx + HEADER_IDX + 2} does not have {e}'
                )
            except Exception as e:
                raise Exception(
                    f'Row {idx + HEADER_IDX + 2} : {e}'
                )

    def run(self):
        """Run the ingestor."""
        if not self.session.file:
            raise FileNotFoundException()

        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            raise Exception(e)
