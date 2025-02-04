# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for date.
"""

from datetime import datetime, timezone


def find_max_min_epoch_dates(min_epoch, max_epoch, epoch):
    """Compare max and min epoch values against epoch.

    :param min_epoch: Min epoch value, could be None
    :type min_epoch: int
    :param max_epoch: Max epoch value, could be None
    :type max_epoch: int
    :param epoch: value to compare against
    :type epoch: int
    :return: Tuple of new (min, max)
    :rtype: int
    """
    min_time = min_epoch
    if min_time is None:
        min_time = epoch
    elif epoch < min_time:
        min_time = epoch

    max_time = max_epoch
    if max_time is None:
        max_time = epoch
    elif epoch > max_time:
        max_time = epoch

    return min_time, max_time


def split_epochs_by_year(start_epoch, end_epoch):
    """Split datetime that is in different year.

    :param start_epoch: Start date time in epoch
    :type start_epoch: int
    :param end_epoch: End date time in epoch
    :type end_epoch: int
    :return: List of (year, start_epoch, end_epoch)
    :rtype: list
    """
    results = []
    start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_epoch, tz=timezone.utc)

    current_year = start_dt.year
    while current_year <= end_dt.year:
        year_start = (
            datetime(current_year, 1, 1, tzinfo=timezone.utc).timestamp()
        )
        year_end = (
            datetime(
                current_year + 1, 1, 1,
                tzinfo=timezone.utc
            ).timestamp() - 1
        )

        start = max(start_epoch, year_start)
        end = min(end_epoch, year_end)

        results.append((current_year, int(start), int(end)))

        current_year += 1

    return results
