# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: UnitTest for Plumber functions.
"""
import os
import mock
import requests_mock
from django.test import TestCase

from spw.utils.plumber import (
    write_plumber_file,
    write_plumber_data,
    remove_plumber_data,
    PLUMBER_PORT,
    plumber_health_check,
    kill_r_plumber_process,
    spawn_r_plumber,
    execute_spw_model
)
from spw.utils.process import write_pidfile
from spw.factories import RModelFactory, RModelOutputFactory


def mocked_os_kill(self, *args, **kwargs):
    """Mock os.kill function."""
    return 1


def find_r_line_code(lines, code):
    """Find code in lines."""
    filtered = [line for line in lines if code in line]
    return len(filtered) > 0


class DummyProcess:
    """Class for dummy process result."""

    def __init__(self, pid):
        """Initialize class with dummy pid."""
        self.pid = pid


def mocked_process(*args, **kwargs):
    """Mock a subprocess."""
    return DummyProcess(1)


class TestPlumberUtils(TestCase):
    """Test plumber utility functions."""

    def test_plumber_health_check(self):
        """Test plumber health check."""
        with requests_mock.Mocker() as m:
            json_response = {'echo': 'ok'}
            m.get(
                f'http://0.0.0.0:{PLUMBER_PORT}/statistical/echo',
                json=json_response,
                headers={'Content-Type': 'application/json'},
                status_code=200
            )
            is_running = plumber_health_check(max_retry=1)
            self.assertTrue(is_running)
        with requests_mock.Mocker() as m:
            json_response = {'echo': 'ok'}
            m.get(
                f'http://0.0.0.0:{PLUMBER_PORT}/statistical/echo',
                json=json_response,
                headers={'Content-Type': 'application/json'},
                status_code=400
            )
            is_running = plumber_health_check(max_retry=1)
            self.assertFalse(is_running)

    @mock.patch('subprocess.Popen',
                mock.Mock(side_effect=mocked_process))
    def test_spawn_r_plumber(self):
        """Test spawn new R plumber process."""
        with requests_mock.Mocker() as m:
            json_response = {'echo': 'ok'}
            m.get(
                f'http://0.0.0.0:{PLUMBER_PORT}/statistical/echo',
                json=json_response,
                headers={'Content-Type': 'application/json'},
                status_code=200
            )
            process = spawn_r_plumber()
        self.assertEqual(process.pid, 1)

    @mock.patch('os.kill')
    def test_kill_r_plumber_process(self, mocked_os):
        """Test killing running R Plumber Procces."""
        mocked_os.side_effect = mocked_os_kill
        pid_path = '/tmp/plumber.pid'
        write_pidfile(26, pid_path)
        kill_r_plumber_process()
        self.assertEqual(mocked_os.call_count, 1)

    def test_execute_spw_model(self):
        """Test execute SPW R Mode."""
        data_filepath = '/tmp/test.csv'
        with requests_mock.Mocker() as m:
            json_response = {'national_trend': 'abcde'}
            m.post(
                f'http://plumber:{PLUMBER_PORT}/spw/generic',
                json=json_response,
                headers={'Content-Type': 'application/json'},
                status_code=200
            )
            is_success, response = execute_spw_model(
                data_filepath, 'test.csv', 0.0, 0.0)
            self.assertTrue(is_success)
            self.assertEqual(response, json_response)
        with requests_mock.Mocker() as m:
            json_response = {'error': 'Internal server error'}
            m.post(
                f'http://plumber:{PLUMBER_PORT}/spw/generic',
                json=json_response,
                headers={'Content-Type': 'application/json'},
                status_code=500
            )
            is_success, response = execute_spw_model(
                data_filepath, 'test.csv', 0.0, 0.0)
            self.assertFalse(is_success)
            self.assertEqual('Internal server error', response['error'])
        with requests_mock.Mocker() as m:
            data_response = 'Test'
            m.post(
                f'http://plumber:{PLUMBER_PORT}/spw/generic',
                json=data_response,
                headers={'Content-Type': 'text/plain'},
                status_code=500
            )
            is_success, response = execute_spw_model(
                data_filepath, 'test.csv')
            self.assertFalse(is_success)
            self.assertEqual('Invalid response content type: text/plain',
                             response)

    def test_write_plumber_file(self):
        """Test writing plumber R file."""
        r_model = RModelFactory.create()
        RModelOutputFactory.create(model=r_model)
        r_file_path = write_plumber_file(
            os.path.join(
                '/tmp',
                'plumber_test.R'
            )
        )
        with open(r_file_path, 'r') as f:
            lines = f.readlines()
        self.assertTrue(find_r_line_code(lines, '@get /statistical/echo'))
        if os.path.exists(r_file_path):
            os.remove(r_file_path)

    def test_manage_plumber_data(self):
        """Test manage plumber data files."""
        headers = ['data', 'count_total']
        csv_data = [
            ['abc', 10],
            ['def', 20]
        ]
        file_path = write_plumber_data(
            headers, csv_data, '/tmp')
        self.assertTrue(os.path.exists(file_path))
        remove_plumber_data(file_path)
        self.assertFalse(os.path.exists(file_path))
