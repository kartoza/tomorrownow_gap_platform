# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Dask Utils.
"""

import dask
from dask.delayed import Delayed
from concurrent.futures import ThreadPoolExecutor

from gap.models import Preferences


def execute_dask_compute(x: Delayed):
    """Execute dask computation based on number of threads config.

    :param x: Dask delayed object
    :type x: Delayed
    """
    num_of_threads = Preferences.load().dask_threads_num
    if num_of_threads <= 0:
        # use everything
        x.compute()
    else:
        with dask.config.set(pool=ThreadPoolExecutor(num_of_threads)):
            x.compute()
