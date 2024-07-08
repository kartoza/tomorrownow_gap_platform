import os
from django.test import TestCase
import mock
from spw.utils.process import (
    write_pidfile,
    kill_process_by_pid
)


class TestUtilsProcess(TestCase):

    @staticmethod
    def mocked_os_kill(self, *args, **kwargs):
        return 1

    @mock.patch('os.kill')
    def test_kill_process_by_pid(self, mocked_os):
        mocked_os.side_effect = TestUtilsProcess.mocked_os_kill
        pid_path = '/tmp/test.pid'
        write_pidfile(26, pid_path)
        self.assertTrue(os.path.exists(pid_path))
        kill_process_by_pid(pid_path)
        self.assertEqual(mocked_os.call_count, 1)
        self.assertFalse(os.path.exists(pid_path))
