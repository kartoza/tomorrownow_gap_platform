# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Dask Utils.
"""

import dask
from dask.delayed import Delayed
from concurrent.futures import ThreadPoolExecutor

from gap.models import Preferences


def execute_dask_compute(x: Delayed, is_api=False):
    """Execute dask computation based on number of threads config.

    :param x: Dask delayed object
    :type x: Delayed
    :param is_api: Whether the computation is in GAP API, default to False
    :type is_api: bool
    """
    preferences = Preferences.load()
    num_of_threads = (
        preferences.dask_threads_num_api if is_api else
        preferences.dask_threads_num
    )
    if num_of_threads <= 0:
        # use everything
        x.compute()
    else:
        with dask.config.set(pool=ThreadPoolExecutor(num_of_threads)):
            x.compute()
