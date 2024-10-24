# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Admin for API Tracking
"""

import random
from django.contrib import admin
from django.db.models import Count, TextField
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import TruncDay, Cast
from rest_framework_tracking.admin import APIRequestLogAdmin
from rest_framework_tracking.models import APIRequestLog as BaseAPIRequestLog

from gap.models import DatasetType
from gap_api.models import APIRequestLog, DatasetTypeAPIConfig


admin.site.unregister(BaseAPIRequestLog)


def generate_random_color():
    """Generate random color for product type."""
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))


class ProductTypeFilter(admin.SimpleListFilter):
    """Custom filter for product type field."""

    title = 'Product Type'
    parameter_name = 'product_type'

    def lookups(self, request, model_admin):
        """Get list of product type."""
        dataset_types = DatasetType.objects.exclude(
            variable_name='default'
        ).order_by('variable_name')
        return [(dt.variable_name, dt.variable_name) for dt in dataset_types]

    def queryset(self, request, queryset):
        """Filter queryset using product type."""
        if self.value():
            return queryset.filter(query_params__product=self.value())
        return queryset


class GapAPIRequestLogAdmin(APIRequestLogAdmin):
    """Admin class for APIRequestLog model."""

    list_display = (
        "id",
        "product_type",
        "requested_at",
        "response_ms",
        "status_code",
        "user",
        "view_method",
        "path",
        "remote_addr",
        "host",
    )
    list_filter = (ProductTypeFilter, "user", "status_code")
    search_fields = ()

    def product_type(self, obj: APIRequestLog):
        """Display product from query_params.

        :param obj: current row
        :type obj: APIRequestLog
        :return: product in json query_params
        :rtype: str
        """
        return obj.query_params.get('product', '-')

    product_type.short_description = 'Product Type'

    def changelist_view(self, request, extra_context=None):
        """Render the changelist view.

        :param request: request
        :type request: Request object
        :param extra_context: extra context, defaults to None
        :type extra_context: any, optional
        :return: Rendered view
        :rtype: any
        """
        # Aggregate api logs per day
        chart_data = self._generate_chart_data(request)

        # generate color for products
        product_counts = []
        for product in chart_data['product']:
            product['color'] = generate_random_color()
            product_counts.append(product)

        extra_context = extra_context or {
            "chart_data": list(chart_data['total_requests']),
            "product_chart_data": product_counts
        }

        # Call the superclass changelist_view to render the page
        return super().changelist_view(request, extra_context=extra_context)

    def _generate_chart_data(self, request):
        """Generate chart data and construct the filter from request object.

        :param request: request
        :type request: Request object
        :return: APIRequestLog group by Date and the count
        :rtype: list
        """
        product_type = request.GET.get("product_type", None)
        user_id = request.GET.get("user__id__exact", None)

        # handle requested_at__day, requested_at__month, requested_at__year
        other_filters = {}
        for key, val in request.GET.items():
            if key.startswith('requested_at__'):
                other_filters[key] = val

        return self._do_query_chart_data(
            product_type, user_id, other_filters)

    def _do_query_chart_data(
            self, product_type, user_id, other_filters):
        """Get chart data by filters.

        :param product_type: product type
        :type product_type: str
        :param user_id: user ID
        :type user_id: int
        :param other_filters: Dictionary of valid filter
        :type other_filters: dict
        :return: APIRequestLog group by Date and the count
        :rtype: list
        """
        filters = {}
        if product_type:
            filters['query_params__product'] = product_type

        if user_id:
            filters['user__id'] = user_id

        filters.update(other_filters)
        return {
            'total_requests': (
                APIRequestLog.objects.filter(
                    **filters
                )
                .annotate(date=TruncDay("requested_at"))
                .values("date")
                .annotate(y=Count("id"))
                .order_by("-date")
            ),
            'product': (
                APIRequestLog.objects.filter(
                    **filters
                ).annotate(
                    product=Cast(
                        KeyTextTransform('product', 'query_params'),
                        TextField()
                    )
                )
                .values('product')
                .annotate(count=Count("id"))
                .order_by('product')
            )
        }


class GapAPIDatasetTypeConfigAdmin(admin.ModelAdmin):
    """Admin class for DatasetTypeAPIConfig."""

    list_display = ('type', 'max_daterange',)


admin.site.register(APIRequestLog, GapAPIRequestLogAdmin)
admin.site.register(DatasetTypeAPIConfig, GapAPIDatasetTypeConfigAdmin)
