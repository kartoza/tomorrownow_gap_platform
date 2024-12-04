# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Dask Utils.
"""

import dask
from dask.delayed import Delayed
from concurrent.futures import ThreadPoolExecutor

from gap.models import Preferences


def get_num_of_threads(is_api=False):
    """Get number of threads for dask computation.

    :param is_api: whether for API usage, defaults to False
    :type is_api: bool, optional
    """
    preferences = Preferences.load()
    return (
        preferences.dask_threads_num_api if is_api else
        preferences.dask_threads_num
    )



def execute_dask_compute(x: Delayed, is_api=False):
    """Execute dask computation based on number of threads config.

    :param x: Dask delayed object
    :type x: Delayed
    :param is_api: Whether the computation is in GAP API, default to False
    :type is_api: bool
    """
    num_of_threads = get_num_of_threads(is_api)
    if num_of_threads <= 0:
        # use everything
        x.compute()
    else:
        with dask.config.set(pool=ThreadPoolExecutor(num_of_threads)):
            x.compute()
