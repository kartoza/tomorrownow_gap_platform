# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Zarr Utilities.
"""

from django.test import TestCase

from gap.models import Preferences
from gap.utils.dask import execute_dask_compute


class MockDelayed(object):
    """Mock class for delayed object."""

    def compute(self):
        """Mock compute method."""
        pass


class TestDaskUtils(TestCase):
    """Unit test for dask utilities."""

    def test_empty_num_of_threads(self):
        """Test num_of_threads is not set."""
        preferences = Preferences.load()
        preferences.dask_threads_num = -1
        preferences.save()
        execute_dask_compute(MockDelayed())

    def test_use_num_of_threads(self):
        """Test num_of_threads is set to 1."""
        preferences = Preferences.load()
        preferences.dask_threads_num = 1
        preferences.save()
        execute_dask_compute(MockDelayed())
