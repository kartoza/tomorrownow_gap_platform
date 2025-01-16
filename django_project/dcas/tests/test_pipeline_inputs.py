# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for DCAS Pipeline Inputs.
"""

import xarray as xr
import numpy as np
import pandas as pd
import datetime
from django.test import TestCase

from dcas.inputs import DCASPipelineInput


class DCASPipelineInputsTest(TestCase):
    """DCAS Pipeline Inputs test case."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json',
        '5.dataset.json',
        '6.unit.json',
        '7.attribute.json',
        '8.dataset_attribute.json'
    ]

    def setUp(self):
        """Set the DCASPipelineInputsTest data."""
        # Create a mock xarray Dataset
        self.dates = pd.date_range(
            datetime.date(2025, 1, 15),
            datetime.date(2025, 1, 15) + datetime.timedelta(days=3),
            freq='d'
        )
        lat = np.linspace(0, 10, 6)  # [0, 2, 4, 6, 8, 10]
        lon = np.linspace(0, 10, 6)  # [0, 2, 4, 6, 8, 10]
        data = np.array([
            [10, 12, 14, 16, 18, 20],  # lat=0
            [20, 22, 24, 26, 28, 30],  # lat=2
            [30, 32, 34, 36, 38, 40],  # lat=4
            [40, 42, 44, 46, 48, 50],  # lat=6
            [50, 52, 54, 56, 58, 60],  # lat=8
            [60, 62, 64, 66, 68, 70],  # lat=10
        ])
        temperature = [data, data, data, data]

        self.ds = xr.Dataset(
            {
                "temperature": (["date", "lat", "lon"], temperature),
                "total_rainfall": (["date", "lat", "lon"], temperature)
            },
            coords={
                "date": self.dates,
                "lat": lat,
                "lon": lon,
            }
        )

        self.input = DCASPipelineInput(datetime.date(2025, 1, 15))
        self.input.setup(datetime.date(2025, 1, 10))

    def test_get_values_at_points(self):
        """Test points interpolation from xarray Dataset."""
        epoch = int(self.dates[0].timestamp())

        # exact points
        in_lat = [2, 4]
        in_lon = [2, 4]
        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature'], {
                'temperature': 'temperature'
            },
            xr.DataArray(in_lat, dims='gdid'),
            xr.DataArray(in_lon, dims='gdid'),
            self.dates[0]
        )
        expected_results = [22, 34]
        np.testing.assert_allclose(
            vap_df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )

        # sel with nearest will result to 2, 6, 10
        in_lat = [1, 5, 9]
        in_lon = [1, 5, 9]

        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature'], {
                'temperature': 'temperature'
            },
            xr.DataArray(in_lat, dims='gdid'),
            xr.DataArray(in_lon, dims='gdid'),
            self.dates[0]
        )

        self.assertIn(f'temperature_{epoch}', vap_df.columns)
        expected_results = [10, 34, 58]
        np.testing.assert_allclose(
            vap_df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )

        # using large diff
        in_lat = [-2, 15]
        in_lon = [-2, 15]
        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature'], {
                'temperature': 'temperature'
            },
            xr.DataArray(in_lat, dims='gdid'),
            xr.DataArray(in_lon, dims='gdid'),
            self.dates[0]
        )
        self.assertTrue(vap_df.isna().all().all())

    def test_multiple_attributes(self):
        """Test selection with multiple attributes."""
        # sel with nearest will result to 2, 6, 10
        in_lat = [1, 5, 9]
        in_lon = [1, 5, 9]

        vap_df = self.input._get_values_at_points(
            self.ds, ['temperature', 'total_rainfall'], {
                'temperature': 'temperature',
                'total_rainfall': 'total_rainfall'
            },
            xr.DataArray(in_lat, dims='gdid'),
            xr.DataArray(in_lon, dims='gdid'),
            self.dates[0]
        )

        epoch = int(self.dates[0].timestamp())
        self.assertIn(f'temperature_{epoch}', vap_df.columns)
        self.assertIn(f'total_rainfall_{epoch}', vap_df.columns)
        expected_results = [10, 34, 58]
        np.testing.assert_allclose(
            vap_df[f'temperature_{epoch}'].values, expected_results, atol=1e-5
        )
        np.testing.assert_allclose(
            vap_df[f'total_rainfall_{epoch}'].values, expected_results,
            atol=1e-5
        )
