# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for API Request Log.
"""

import json
import mock
import datetime
from django.test import TestCase, RequestFactory
from django.contrib.admin import ModelAdmin
from django.core.cache import cache

from core.factories import UserF
from gap.models import DatasetType
from gap_api.models import APIRequestLog
from gap_api.mixins import GAPAPILoggingMixin
from gap_api.tasks import store_api_logs
from gap_api.admin import ProductTypeFilter, GapAPIRequestLogAdmin
from gap_api.factories import APIRequestLogFactory


class MockRequestObj(object):
    """Mock GET request object."""

    GET = {}


class TestAPIRequestLog(TestCase):
    """Test class for APIRequestLog model and task."""

    fixtures = [
        '2.provider.json',
        '3.station_type.json',
        '4.dataset_type.json'
    ]

    def setUp(self):
        """Initialize test class."""
        self.factory = RequestFactory()
        self.user = UserF.create()
        self.admin_instance = GapAPIRequestLogAdmin(
            APIRequestLog, admin_site=mock.MagicMock())

    def test_store_api_logs(self):
        """Test store api logs from cache."""
        cache._cache.get_client().rpush(
            GAPAPILoggingMixin.CACHE_KEY,
            json.dumps({
                "requested_at": "2024-10-17 17:56:52.270889+00:00",
                "data": None,
                "remote_addr": "127.0.0.1",
                "view": "gap_api.api_views.measurement.MeasurementAPI",
                "view_method": "get",
                "path": "/api/v1/measurement/",
                "host": "localhost:8000",
                "user_agent": "PostmanRuntime/7.42.0",
                "method": "GET",
                "query_params": {
                    "lat": "-1.404244",
                    "lon": "35.008688",
                    "attributes": (
                        "max_relative_humidity,min_relative_humidity"
                    ),
                    "start_date": "2019-11-01",
                    "end_date": "2019-11-01",
                    "product": "tahmo_ground_observation",
                    "output_type": "csv"
                },
                "user": f"{self.user.id}",
                "username_persistent": "admin",
                "response_ms": 182,
                "response": None,
                "status_code": 200
            })
        )
        store_api_logs()
        logs = APIRequestLog.objects.all()
        self.assertEqual(logs.count(), 1)
        self.assertEqual(logs.first().user, self.user)

    def test_product_type_filter(self):
        """Test ProductTypeFilter class."""
        f = ProductTypeFilter(None, {}, APIRequestLog, self.admin_instance)
        names = f.lookups(None, self.admin_instance)
        self.assertEqual(
            len(names),
            DatasetType.objects.exclude(
                variable_name='default'
            ).count()
        )

    def test_admin_product_type(self):
        """Test product_type field in Admin class."""
        f = ProductTypeFilter(None, {}, APIRequestLog, self.admin_instance)
        f.used_parameters = {}

        # create two logs
        log1 = APIRequestLogFactory.create(
            query_params={
                'product': 'cbam_reanalysis_data'
            }
        )
        APIRequestLogFactory.create()

        qs = APIRequestLog.objects.all()

        # query with empty param should return all
        res = f.queryset(None, qs)
        self.assertEqual(res.count(), 2)

        # query with product_type cbam should return 1
        f.used_parameters = {
            'product_type': 'cbam_reanalysis_data'
        }
        res = f.queryset(None, qs)
        self.assertEqual(res.count(), 1)
        self.assertEqual(res.first().id, log1.id)

    @mock.patch.object(GapAPIRequestLogAdmin, '_generate_chart_data')
    @mock.patch.object(ModelAdmin, 'changelist_view')
    def test_change_list_view(
            self, mock_super_changelist_view, mock_generate_chart_data):
        """Test change_list view."""
        mock_chart_data = [{"date": "2024-10-18", "count": 10}]
        mock_generate_chart_data.return_value = mock_chart_data

        # Simulate a GET request to the changelist view
        request = self.factory.get('/admin/gap_api/apirequestlog/')

        # Call the changelist_view method
        self.admin_instance.changelist_view(request)

        # Assert that _generate_chart_data was called
        mock_generate_chart_data.assert_called_once_with(request)

        # Assert that the extra_context includes the correct chart data
        expected_extra_context = {"chart_data": list(mock_chart_data)}
        mock_super_changelist_view.assert_called_once_with(
            request, extra_context=expected_extra_context)

    def test_generate_chart_data(self):
        """Test generate chart data."""
        APIRequestLogFactory.create(
            requested_at=datetime.datetime(2024, 10, 1, 0, 0, 0),
            user=self.user
        )
        APIRequestLogFactory.create(
            requested_at=datetime.datetime(2023, 5, 1, 0, 0, 0),
            user=self.user
        )
        req = MockRequestObj()
        req.GET = {
            'product_type': 'tahmo_ground_observation',
            'requested_at__year': '2023',
            'user__id__exact': f'{self.user.id}'
        }
        data = self.admin_instance._generate_chart_data(req)
        self.assertEqual(len(data), 1)
