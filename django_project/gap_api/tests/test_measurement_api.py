# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

from django.urls import reverse

from core.tests.common import FakeResolverMatchV1, BaseAPIViewTest
from gap_api.api_views.measurement import HistoricalAPI, ForecastAPI


class CommonMeasurementAPITest(BaseAPIViewTest):
    """Common class for Measurement API Test."""

    def _get_measurement_request(
            self, lat=-2.215, lon=29.125, attributes='max_total_temperature',
            start_dt='2024-04-01', end_dt='2024-04-04'):
        """Get request for Measurement API.

        :param lat: latitude, defaults to -2.215
        :type lat: float, optional
        :param lon: longitude, defaults to 29.125
        :type lon: float, optional
        :param attributes: comma separated list of attribute,
            defaults to 'max_total_temperature'
        :type attributes: str, optional
        :param start_dt: start date range, defaults to '2024-04-01'
        :type start_dt: str, optional
        :param end_dt: end date range, defaults to '2024-04-04'
        :type end_dt: str, optional
        :return: Request object
        :rtype: WSGIRequest
        """
        request = self.factory.get(
            reverse('api:v1:user-info') +
            f'?lat={lat}&lon={lon}&attributes={attributes}'
            f'&start_date={start_dt}&end_date={end_dt}'
        )
        request.user = self.superuser
        request.resolver_match = FakeResolverMatchV1
        return request


class HistoricalAPITest(CommonMeasurementAPITest):
    """Historical api test case."""

    def test_read_historical_data_empty(self):
        """Test read historical data that returns empty."""
        view = HistoricalAPI.as_view()
        request = self._get_measurement_request()
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {})



class ForecastAPITest(CommonMeasurementAPITest):
    """Forecast api test case."""

    def test_read_forecast_data_empty(self):
        """Test read forecast data that returns empty."""
        view = ForecastAPI.as_view()
        request = self._get_measurement_request()
        response = view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {})
