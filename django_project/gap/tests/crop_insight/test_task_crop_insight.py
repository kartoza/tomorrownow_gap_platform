# coding=utf-8
"""Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

import datetime
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from core.factories import BackgroundTaskF
from core.models.background_task import TaskStatus
from gap.factories import CropInsightRequestFactory
from gap.models import CropInsightRequest


class CropInsideTaskRUDTest(TestCase):
    """Crop test case."""

    Factory = CropInsightRequestFactory
    Model = CropInsightRequest

    def test_today_reports(self):
        """Test query today reports."""
        now = timezone.now()
        self.Factory(requested_date=now)
        self.Factory(requested_date=now)
        self.Factory(requested_date=now + datetime.timedelta(days=-1))
        self.assertEqual(CropInsightRequest.today_reports().count(), 2)

    @patch('gap.models.crop_insight.CropInsightRequest._generate_report')
    def test_is_running(self, mock_generate_report):
        """Test skip run."""
        # No skip running of no bg task
        report = self.Factory()
        self.assertFalse(report.skip_run)

        # -----------------------------------------------------
        # For first background, everything are run
        # -----------------------------------------------------
        # No skip running if bg task is still PENDING
        bg_task_1 = BackgroundTaskF.create(
            task_name='generate_crop_plan',
            context_id=report.id
        )
        bg_task_1.status = TaskStatus.PENDING
        bg_task_1.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 1)

        # Skip running if bg task is QUEUED
        bg_task_1.status = TaskStatus.QUEUED
        report.run()
        self.assertEqual(mock_generate_report.call_count, 2)

        # Skip running if bg task is still RUNNING
        bg_task_1.status = TaskStatus.RUNNING
        report.run()
        self.assertEqual(mock_generate_report.call_count, 3)

        # Skip running if bg task is COMPLETED
        bg_task_1.status = TaskStatus.COMPLETED
        report.run()
        self.assertEqual(mock_generate_report.call_count, 4)

        # No skip running if bg task is CANCELLED
        bg_task_1.status = TaskStatus.CANCELLED
        report.run()
        self.assertEqual(mock_generate_report.call_count, 5)

        # No skip running if bg task is STOPPED
        bg_task_1.status = TaskStatus.STOPPED
        report.run()
        self.assertEqual(mock_generate_report.call_count, 6)

        # No skip running if bg task is INVALIDATED
        bg_task_1.status = TaskStatus.INVALIDATED
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # -----------------------------------------------------
        # The second one is skipped if first one is still running
        # -----------------------------------------------------
        # No skip running if bg task is still PENDING
        bg_task_1.status = TaskStatus.RUNNING
        bg_task_1.save()
        bg_task_2 = BackgroundTaskF.create(
            task_name='generate_crop_plan',
            context_id=report.id
        )
        bg_task_2.status = TaskStatus.PENDING
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # Skip running if bg task is QUEUED
        bg_task_2.status = TaskStatus.QUEUED
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # Skip running if bg task is still RUNNING
        bg_task_2.status = TaskStatus.RUNNING
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # Skip running if bg task is COMPLETED
        bg_task_2.status = TaskStatus.COMPLETED
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # No skip running if bg task is CANCELLED
        bg_task_2.status = TaskStatus.CANCELLED
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # No skip running if bg task is STOPPED
        bg_task_2.status = TaskStatus.STOPPED
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)

        # No skip running if bg task is INVALIDATED
        bg_task_2.status = TaskStatus.INVALIDATED
        bg_task_2.save()
        report.run()
        self.assertEqual(mock_generate_report.call_count, 7)
