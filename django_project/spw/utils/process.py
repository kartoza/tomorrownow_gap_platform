"""Utilities function for process management."""
import os
import logging
from signal import SIGKILL


logger = logging.getLogger(__name__)


def write_pidfile(pid, pidfile_path):
    """Write pid to file."""
    with open(pidfile_path, 'w', encoding='utf-8') as f:
        f.write(str(pid))


def read_pid_from_pidfile(pidfile_path):
    """ Read the PID recorded in the named PID file.

        Read and return the numeric PID recorded as text in the named
        PID file. If the PID file cannot be read, or if the content is
        not a valid PID, return ``None``.

        """
    pid = None
    try:
        pidfile = open(pidfile_path, 'r')
    except IOError as ex:
        logger.error(ex)
    else:
        # According to the FHS 2.3 section on PID files in /var/run:
        #
        #   The file must consist of the process identifier in
        #   ASCII-encoded decimal, followed by a newline character.
        #
        #   Programs that read PID files should be somewhat flexible
        #   in what they accept; i.e., they should ignore extra
        #   whitespace, leading zeroes, absence of the trailing
        #   newline, or additional lines in the PID file.

        line = pidfile.readline().strip()
        try:
            pid = int(line)
        except ValueError as ex:
            logger.error(ex)
        pidfile.close()

    return pid


def kill_process_by_pid(pidfile_path):
    """Kill process by PID. """
    plumber_pid = read_pid_from_pidfile(pidfile_path)
    logger.info(f'Killing pid {plumber_pid}')
    if plumber_pid:
        # kill a process via pid
        os.kill(plumber_pid, SIGKILL)
        try:
            os.remove(pidfile_path)
        except IOError as ex:
            logger.error(ex)
