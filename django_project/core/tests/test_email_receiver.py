# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase

from core.factories import UserF
from core.group_email_receiver import (
    _group_crop_plan_receiver, crop_plan_receiver
)


class CropPlanReceiverTest(TestCase):
    """Crop plan receiver tests."""

    def test_crop_plan_receiver(self):
        """Test crop plan receiver."""
        group = _group_crop_plan_receiver()
        user_1 = UserF()
        user_2 = UserF()
        user_3 = UserF()
        group.user_set.add(user_1, user_2)

        self.assertEqual(
            crop_plan_receiver().count(), 2
        )
        self.assertTrue(
            user_3.pk not in crop_plan_receiver().values_list('pk', flat=True)
        )
