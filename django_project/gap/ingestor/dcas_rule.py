# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm ingestor.
"""

import pandas as pd

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException
)
from gap.models import IngestorSession


class Keys:
    """Keys for the data."""

    CROP = 'crop'
    PARAMETER = 'parameter'
    GROWTH_STAGE = 'growth_stage'
    MIN_RANGE = 'min_range'
    MAX_RANGE = 'max_range'
    CODE = 'code'


class DcasRuleIngestor(BaseIngestor):
    """Ingestor for DCAS Rules."""

    def __init__(self, session: IngestorSession, working_dir: str = '/tmp'):
        """
        Initialize the ingestor.

        Preprosessing the original excel to csv:
        - Replace Inf with 999999
        - Remove columns other than Keys
        - Rename headers to Keys
        """
        super().__init__(session, working_dir)

    def _run(self):
        df = pd.read_csv(
            self.session.file.read(),
            converters={
                Keys.CODE: str,
            }
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
