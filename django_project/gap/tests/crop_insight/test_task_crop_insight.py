# coding=utf-8
"""Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

import datetime

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

    def test_is_running(self):
        """Test skip run."""
        now = timezone.now()

        # No skip running of no bg task
        obj = self.Factory(requested_date=now.date())
        self.assertFalse(obj.skip_run)

        # No skip running if bg task is still PENDING
        bg_task = BackgroundTaskF.create(
            task_name='generate_crop_plan',
            context_id=obj.id
        )
        bg_task.status = TaskStatus.PENDING
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # Skip running if bg task is QUEUED
        bg_task.status = TaskStatus.QUEUED
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # Skip running if bg task is still RUNNING
        bg_task.status = TaskStatus.RUNNING
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # Skip running if bg task is COMPLETED
        bg_task.status = TaskStatus.COMPLETED
        bg_task.save()
        self.assertTrue(obj.skip_run)

        # No skip running if bg task is CANCELLED
        bg_task.status = TaskStatus.CANCELLED
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # No skip running if bg task is STOPPED
        bg_task.status = TaskStatus.STOPPED
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # No skip running if bg task is INVALIDATED
        bg_task.status = TaskStatus.INVALIDATED
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # Has second task, the first task is running
        # No skip running if bg task is still PENDING
        bg_task.status = TaskStatus.RUNNING
        bg_task.save()
        bg_task = BackgroundTaskF.create(
            task_name='generate_crop_plan',
            context_id=obj.id
        )
        bg_task.status = TaskStatus.PENDING
        bg_task.save()
        self.assertTrue(obj.skip_run)

        # Skip running if bg task is QUEUED
        bg_task.status = TaskStatus.QUEUED
        bg_task.save()
        self.assertTrue(obj.skip_run)

        # Skip running if bg task is still RUNNING
        bg_task.status = TaskStatus.RUNNING
        bg_task.save()
        self.assertTrue(obj.skip_run)

        # Skip running if bg task is COMPLETED
        bg_task.status = TaskStatus.COMPLETED
        bg_task.save()
        self.assertTrue(obj.skip_run)

        # No skip running if bg task is CANCELLED
        bg_task.status = TaskStatus.CANCELLED
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # No skip running if bg task is STOPPED
        bg_task.status = TaskStatus.STOPPED
        bg_task.save()
        self.assertFalse(obj.skip_run)

        # No skip running if bg task is INVALIDATED
        bg_task.status = TaskStatus.INVALIDATED
        bg_task.save()
        self.assertFalse(obj.skip_run)
