# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for BackgroundTask Model.
"""

import mock
import datetime
import pytz
from django.test import TestCase
from ast import literal_eval as make_tuple

from core.models import BackgroundTask, TaskStatus
from core.celery import (
    cancel_task,
    check_ongoing_task,
    update_task_progress,
    is_task_ignored,
    task_sent_handler,
    task_received_handler,
    task_prerun_handler,
    task_success_handler,
    task_failure_handler,
    task_revoked_handler,
    task_internal_error_handler,
    task_retry_handler,
    conf
)
from core.factories import BackgroundTaskF, UserF


mocked_dt = datetime.datetime(2024, 8, 14, 10, 10, 10, tzinfo=pytz.UTC)


class RequestObj(object):
    """Mock task request object."""

    def __init__(self, id, name, task_args) -> None:
        """Initialize RequestObj class."""
        self.id = id
        self.name = name
        self.args = task_args


class SenderObj(object):
    """Mock task sender object."""

    def __init__(self, name, request) -> None:
        """Initialize SenderObj class."""
        self.name = name
        self.request = request


@mock.patch(
    'django.utils.timezone.now',
    mock.MagicMock(
        return_value=mocked_dt
    )
)
class TestBackgroundTask(TestCase):
    """Test class for BackgroundTask and its handlers."""

    def setUp(self):
        """Initialize BackgroundTask test class."""
        self.bg_task: BackgroundTask = BackgroundTaskF.create(
            task_name='test-task-1',
            context_id='1'
        )

    def get_task_sender(self, bg_task: BackgroundTask):
        """Create sender object from BackgroundTask."""
        request = RequestObj(
            bg_task.task_id,
            bg_task.task_name,
            make_tuple(bg_task.parameters)
        )
        return SenderObj(request.name, request)

    def test_requester_name(self):
        """Test BackgroundTask requester name property."""
        self.assertEqual(self.bg_task.requester_name, 'John Doe')
        user = UserF.create(
            first_name='Test',
            last_name=""
        )
        bg1: BackgroundTask = BackgroundTaskF.create(
            submitted_by=user
        )
        self.assertEqual(bg1.requester_name, 'Test')
        bg2: BackgroundTask = BackgroundTaskF.create(
            submitted_by=None
        )
        self.assertEqual(bg2.requester_name, '-')

    def test_task_on_sent(self):
        pass

    def test_task_on_queued(self):
        pass

    def test_task_on_started(self):
        pass

    def test_task_on_completed(self):
        pass

    def test_task_on_cancelled(self):
        pass

    def test_task_on_errors(self):
        pass

    def test_task_on_retried(self):
        pass

    def test_is_possible_interrupted(self):
        pass

    def test_conf_celery(self):
        """Test celery conf."""
        conf_data = conf("test")
        self.assertIn('error', conf_data)

    @mock.patch('core.celery.logger.error')
    @mock.patch("core.celery.AsyncResult")
    @mock.patch("core.celery.app.control.revoke")
    def test_cancel_task(self, mocked_revoke, mock_async_result, mock_log):
        """Test cancel task function."""
        mock_result = mock.MagicMock()
        mock_result.ready.return_value = True
        mock_async_result.return_value = mock_result
        cancel_task("empty")
        mocked_revoke.assert_not_called()
        mock_log.assert_not_called()
        # test revoke is called
        mocked_revoke.reset_mock()
        mock_result = mock.MagicMock()
        mock_result.ready.return_value = False
        mock_async_result.return_value = mock_result
        cancel_task("empty")
        mocked_revoke.assert_called_once()
        mock_log.assert_not_called()
        # test throws exception
        mocked_revoke.reset_mock()
        mock_result = mock.MagicMock()
        mock_result.ready.return_value = False
        mock_async_result.return_value = mock_result
        mocked_revoke.side_effect = Exception('Revoke failed')
        cancel_task("empty")
        self.assertEqual(mock_log.call_count, 2)

    def test_check_ongoing_task(self):
        """Test check ongoing task function."""
        self.assertFalse(
            check_ongoing_task("empty", "empty")
        )
        self.assertFalse(
            check_ongoing_task(
                self.bg_task.task_name, self.bg_task.context_id)
        )
        self.bg_task.status = TaskStatus.RUNNING
        self.bg_task.save()
        self.assertTrue(
            check_ongoing_task(
                self.bg_task.task_name, self.bg_task.context_id)
        )

    def test_update_task_progress(self):
        """Test update task progress function."""
        update_task_progress("empty", "empty", 0, None)
        update_task_progress(
            self.bg_task.task_name,
            self.bg_task.context_id,
            77,
            'update-progress'
        )
        self.bg_task.refresh_from_db()
        self.assertEqual(
            self.bg_task.progress, 77
        )
        self.assertEqual(
            self.bg_task.progress_text, 'update-progress'
        )
        self.assertEqual(
            self.bg_task.last_update, mocked_dt
        )

    def test_is_task_ignored(self):
        """Test is task ignored."""
        self.assertTrue(
            is_task_ignored("")
        )
        self.assertTrue(
            is_task_ignored("celery.backend_cleanup")
        )
        self.assertFalse(
            is_task_ignored("test-new-task")
        )
