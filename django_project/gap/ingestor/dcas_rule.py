# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm ingestor.
"""

import pandas as pd
import io
from django.db.models import Q

from gap.ingestor.base import BaseIngestor
from gap.ingestor.exceptions import (
    FileNotFoundException, FileIsNotCorrectException,
    AdditionalConfigNotFoundException
)
from gap.models import (
    IngestorSession, Attribute,
    Crop, CropGrowthStage, CropStageType
)
from dcas.models.config import DCASConfig
from dcas.models.rule import DCASRule


class Keys:
    """Keys for the data."""

    CROP = 'crop'
    PARAMETER = 'parameter'
    GROWTH_STAGE = 'growth_stage'
    MIN_RANGE = 'min_range'
    MAX_RANGE = 'max_range'
    CODE = 'code'

    @staticmethod
    def check_columns(df) -> bool:
        """Check if all columns exist in dataframe.

        :param df: dataframe from csv
        :type df: pd.DataFrame
        :raises FileIsNotCorrectException: When column is missing
        """
        keys = [
            Keys.CROP, Keys.PARAMETER, Keys.GROWTH_STAGE,
            Keys.MIN_RANGE, Keys.MAX_RANGE, Keys.CODE
        ]

        missing = []
        for key in keys:
            if key not in df.columns:
                missing.append(key)

        if missing:
            raise FileIsNotCorrectException(
                f'Column(s) missing: {",".join(missing)}'
            )


class DcasRuleIngestor(BaseIngestor):
    """Ingestor for DCAS Rules."""

    BATCH_SIZE = 500

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
        rule_config_id = self.get_config('rule_config_id')
        if rule_config_id is None:
            raise AdditionalConfigNotFoundException('rule_config_id')

        rule_config = DCASConfig.objects.get(id=rule_config_id)

        # clear existing rules
        DCASRule.objects.filter(
            config=rule_config
        ).delete()

        df = pd.read_csv(
            io.BytesIO(self.session.file.read()),
            converters={
                Keys.CODE: str,
            }
        )
        # validate columns
        Keys.check_columns(df)

        try:
            rules = []
            for idx, row in df.iterrows():
                # get crop and stage type
                crop_with_stage = row[Keys.CROP].lower().split('_')
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

                # get crop growth stage
                growth_stage = CropGrowthStage.objects.get(
                    name__iexact=row[Keys.GROWTH_STAGE]
                )

                # get attribute
                attribute = Attribute.objects.get(
                    name__iexact=row[Keys.PARAMETER]
                )

                rules.append(
                    DCASRule(
                        config=rule_config,
                        crop=crop,
                        crop_stage_type=stage_type,
                        crop_growth_stage=growth_stage,
                        parameter=attribute,
                        min_range=row[Keys.MIN_RANGE],
                        max_range=row[Keys.MAX_RANGE],
                        code=row[Keys.CODE]
                    )
                )

                if len(rules) >= self.BATCH_SIZE:
                    DCASRule.objects.bulk_create(rules)
                    rules.clear()

            if rules:
                DCASRule.objects.bulk_create(rules)
                rules.clear()
        except Exception as ex:
            DCASRule.objects.filter(
                config=rule_config
            ).delete()
            raise Exception(
                f'Row {idx} : {ex}'
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
