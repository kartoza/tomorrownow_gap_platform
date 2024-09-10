# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo ingestor.
"""

import pandas as pd
from django.contrib.gis.geos import WKTReader, GEOSException

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException
)
from gap.models import IngestorSession, Grid, Country

HEADER_IDX = 0


class Keys:
    """Keys for the data."""

    UNIQUE_ID = 'locationid'
    ELEVATION = 'elevation'
    SHAPEWKT = 'shapewkt'
    NAME = 'name'


class GridIngestor(BaseIngestor):
    """Ingestor for grid data."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """Initialize the ingestor."""
        super().__init__(session, working_dir)

    def _run(self):
        """Run the ingestor."""
        try:
            df = pd.read_csv(
                self.session.file,
                converters={
                    Keys.UNIQUE_ID: str
                }
            )
        except ValueError as e:
            if 'invalid continuation byte' in f'{e}':
                raise Exception('File should be csv')
            else:
                raise e

        df.reset_index(drop=True, inplace=True)
        data = df.to_dict(orient='records')

        # Process the farm
        total = len(data)
        for idx, row in enumerate(data):
            try:
                unique_id = row[Keys.UNIQUE_ID]
                elevation = row[Keys.ELEVATION]
                shapewkt = row[Keys.SHAPEWKT]
                geometry = WKTReader().read(shapewkt)
                name = row[Keys.NAME]
                grid, _ = Grid.objects.update_or_create(
                    unique_id=unique_id,
                    geometry=geometry,
                    defaults={
                        'name': name,
                        'elevation': elevation
                    }
                )
                if not grid.country:
                    try:
                        grid.country = Country.get_countries_by_polygon(
                            geometry
                        )[0]
                        grid.save()
                    except IndexError:
                        pass
            except KeyError as e:
                raise FileIsNotCorrectException(
                    f'Row {idx + HEADER_IDX + 2} does not have {e}'
                )
            except GEOSException as e:
                raise Exception(
                    f'Row {idx + HEADER_IDX + 2} : wkt is not correct'
                )
            except Exception as e:
                raise Exception(
                    f'Row {idx + HEADER_IDX + 2} : {e}'
                )

            self.session.notes = f'{idx + 1}/{total}'
            self.session.save()

    def run(self):
        """Run the ingestor."""
        if not self.session.file:
            raise FileNotFoundException()

        # Run the ingestion
        try:
            self._run()
        except Exception as e:
            raise Exception(e)
