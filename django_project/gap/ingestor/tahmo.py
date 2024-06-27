# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tahmo ingestor.
"""

import os
import shutil
from zipfile import ZipFile

from gap.ingestor.exceptions import FileNotFoundException
from gap.models.ingestor import IngestorSession


class TahmoVariable:
    """Contains Tahmo Variable."""

    def __init__(self, name, unit=None):
        """Initialize the Tahmo variable."""
        self.name = name
        self.unit = unit


TAHMO_VARIABLES = {
    'ap': TahmoVariable('Atmospheric pressure'),
    'pr': TahmoVariable('Precipitation', 'mm'),
    'rh': TahmoVariable('Relative humidity'),
    'ra': TahmoVariable('Shortwave radiation', 'W/m2'),
    'te': TahmoVariable('Surface air temperature', '°C'),
    'wd': TahmoVariable('Wind direction', 'Degrees from North'),
    'wg': TahmoVariable('Wind gust', 'm/s'),
    'ws': TahmoVariable('Wind speed', 'm/s')
}


class TahmoIngestor:
    """Ingestor for tahmo data."""

    def __init__(self, session: IngestorSession):
        """Initialize the ingestor."""
        self.session = session

    def _run(self):
        """Run the ingestor."""
        pass

    def run(self):
        """Run the ingestor."""
        if not self.session.file:
            raise FileNotFoundException()

        # Extract file
        dir_path = os.path.splitext(self.session.file.path)[0]
        with ZipFile(self.session.file.path, 'r') as zip_ref:
            zip_ref.extractall(dir_path)

        # Run the ingestion
        try:
            self._run()
            shutil.rmtree(dir_path)
        except Exception as e:
            shutil.rmtree(dir_path)
            raise Exception(e)
