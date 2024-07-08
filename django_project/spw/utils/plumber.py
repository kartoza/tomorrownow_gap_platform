import logging
import traceback
import os
import time
import csv
import subprocess
import requests
from uuid import uuid4

from core.settings.utils import absolute_path
from spw.utils.process import (
    write_pidfile,
    kill_process_by_pid
)

logger = logging.getLogger(__name__)
PLUMBER_PORT = os.getenv('PLUMBER_PORT', 8282)


def plumber_health_check(max_retry=5):
    """
    Check whether API is up and running.

    This will be called from worker.
    :param max_retry: maximum retry of checking
    :return: True if successful number of check is less than max_retry
    """
    request_url = f'http://0.0.0.0:{PLUMBER_PORT}/statistical/echo'
    retry = 0
    req = None
    time.sleep(1)
    while (req is None or req.status_code != 200) and retry < max_retry:
        try:
            req = requests.get(request_url)
            if req.status_code != 200:
                time.sleep(2)
            else:
                break
        except Exception as ex:  # noqa
            logger.error(ex)
            time.sleep(2)
        retry += 1
    if retry < max_retry:
        logger.info('Plumber API is up and running!')
    return retry < max_retry


def spawn_r_plumber():
    """Run a Plumber API server."""
    command_list = (
        [
            'R',
            '-e',
            (
                "pr <- plumber::plumb("
                f"'/home/web/plumber_data/plumber.R'); "
                f"args <- list(host = '0.0.0.0', port = {PLUMBER_PORT}); "
                "do.call(pr$run, args)"
            )
        ]
    )
    logger.info('Starting plumber API')
    process = None
    try:
        # redirect stdout and stderr
        with open('/proc/1/fd/1', 'w') as fd:
            process = subprocess.Popen(
                command_list,
                stdout=fd,
                stderr=fd
            )
        # sleep for 10 seconds to wait the API is up
        time.sleep(10)
        # we can also use polling to echo endpoint for health check
        plumber_health_check()
        # write process pid to /tmp/
        write_pidfile(process.pid, '/tmp/plumber.pid')
    except Exception as ex:  # noqa
        logger.error(ex)
        logger.error(traceback.format_exc())
        if process:
            process.terminate()
            process = None
    return process


def kill_r_plumber_process():
    """Kill plumber process by PID stored in file."""
    pid_path = os.path.join(
        '/',
        'tmp',
        'plumber.pid'
    )
    kill_process_by_pid(pid_path)


def execute_spw_model(data_filepath):
    api_name = f'api_spw'
    request_url = f'http://plumber:{PLUMBER_PORT}/statistical/{api_name}'
    data = {
        'filepath': data_filepath
    }
    response = requests.post(request_url, data=data)
    content_type = response.headers['Content-Type']
    error = None
    if content_type == 'application/json':
        if response.status_code == 200:
            return True, response.json()
        else:
            logger.error(
                f'Plumber error response: {str(response.json())}')
            error = response.json()
    else:
        logger.error(f'Invalid response content type: {content_type}')
        error = f'Invalid response content type: {content_type}'
    return False, error


def write_plumber_file(file_path = None):
    """Write R codes to plumber.R"""
    r_file_path = file_path if file_path else os.path.join(
        '/home/web/plumber_data',
        'plumber.R'
    )
    template_file = absolute_path(
        'spw', 'utils', 'plumber_template.R'
    )
    with open(template_file, 'r') as f:
        lines = f.readlines()
    # TODO: write SPW R Code here
    with open(r_file_path, 'w') as f:
        for line in lines:
            f.write(line)
    return r_file_path


def write_plumber_data(headers, csv_data, dir_path='/home/web/plumber_data'):
    """
    Write csv data to file in plumber_data.

    :param headers: list of header name
    :param csv_data: list of row
    :return: file path of exported csv
    """
    r_data_path = os.path.join(
        dir_path,
        f'{str(uuid4())}.csv'
    )
    with open(r_data_path, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(headers)
        writer.writerows(csv_data)
    return r_data_path


def remove_plumber_data(data_filepath):
    """
    Remove csv data file.

    :param data_filepath: filepath to the csv file
    """
    try:
        if os.path.exists(data_filepath):
            os.remove(data_filepath)
    except Exception as ex:
        logger.error(ex)
