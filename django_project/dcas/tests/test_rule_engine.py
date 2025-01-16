# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS RuleEngine.
"""

import numpy as np
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from core.settings.utils import absolute_path
from gap.models import (
    IngestorSession, IngestorType,
    Attribute, CropGrowthStage, CropStageType, Crop
)
from dcas.models import DCASConfig
from dcas.rules.rule_engine import DCASRuleEngine
from dcas.rules.variables import DCASData


class DCASRuleEngineTest(TestCase):
    """DCAS Rule Engine test case."""

    fixtures = [
        '6.unit.json',
        '7.attribute.json',
        '12.crop_stage_type.json',
        '13.crop_growth_stage.json',
        '1.dcas_config.json'
    ]

    def setUp(self):
        """Set the test class."""
        self.default_config = DCASConfig.objects.get(id=1)
        self._ingest_rule()

    def _ingest_rule(self):
        """Ingest ruleset."""
        file_path = absolute_path(
            'gap', 'tests', 'ingestor', 'data', 'dcas', 'valid.csv'
        )
        _file = open(file_path, 'rb')
        session = IngestorSession.objects.create(
            file=SimpleUploadedFile(_file.name, _file.read()),
            ingestor_type=IngestorType.DCAS_RULE,
            additional_config={
                'rule_config_id': self.default_config.id
            },
            trigger_task=False
        )
        session.run()
        session.delete()

    def test_execute_rule(self):
        """Test rule execution."""
        rule_engine = DCASRuleEngine(self.default_config)
        rule_engine.initialize()

        # get data
        parameter = Attribute.objects.get(name='P/PET')
        growth_stage = CropGrowthStage.objects.get(name='Germination')
        stage_type = CropStageType.objects.get(name='Early')
        crop = Crop.objects.get(name='Cassava')

        # execute rule
        param = {
            'id': parameter.id,
            'value': 0.5
        }
        data = DCASData(crop.id, stage_type.id, growth_stage.id, [param])
        rule_engine.execute_rule(data)

        # assert
        self.assertEqual(len(data.message_codes), 1)
        self.assertIn('202400000', data.message_codes)

        # message not found
        param = {
            'id': parameter.id,
            'value': 999
        }
        data = DCASData(crop.id, stage_type.id, growth_stage.id, [param])
        rule_engine.execute_rule(data)

        # assert
        self.assertEqual(len(data.message_codes), 0)

        # test value with nan/inf
        param = {
            'id': parameter.id,
            'value': np.nan
        }
        data = DCASData(crop.id, stage_type.id, growth_stage.id, [param])
        rule_engine.execute_rule(data)

        # assert
        self.assertEqual(len(data.message_codes), 1)
        self.assertIn('202400000', data.message_codes)

        param = {
            'id': parameter.id,
            'value': np.inf
        }
        data = DCASData(crop.id, stage_type.id, growth_stage.id, [param])
        rule_engine.execute_rule(data)

        # assert
        self.assertEqual(len(data.message_codes), 0)
