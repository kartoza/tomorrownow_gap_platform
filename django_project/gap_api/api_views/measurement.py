# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

from datetime import date, datetime, time, timedelta
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
from django.contrib.gis.geos import Point

from gap.models import (
    Provider,
    Attribute,
    NetCDFFile
)
from gap.utils.netcdf import (
    read_value,
    check_netcdf_variables
)
from gap_api.serializers.common import APIErrorSerializer
from gap_api.utils.helper import ApiTag


def daterange_inc(start_date: date, end_date: date):
    """Iterate through start_date and end_date (inclusive).

    :param start_date: start date
    :type start_date: date
    :param end_date: end date inclusive
    :type end_date: date
    :yield: iteration date
    :rtype: date
    """
    days = int((end_date - start_date).days)
    for n in range(days + 1):
        yield start_date + timedelta(n)


class MeasurementAPI(APIView):
    """Fetch measurement by attribute and date range."""

    permission_classes = [IsAuthenticated]
    date_format = '%Y-%m-%d'

    def _get_attribute_filter(self):
        attribute_str = self.request.GET.get('attribute')
        return get_object_or_404(Attribute, name=attribute_str)

    def _get_date_filter(self, attr_name):
        """Get date object from filter (start_date/end_date).

        :param attr_name: request parameter name
        :type attr_name: str
        :return: Date object
        :rtype: date
        """
        date_str = self.request.GET.get(attr_name, None)
        return (
            date.today() if date_str is None else
            datetime.strptime(date_str, self.date_format).date()
        )

    def _get_location_filter(self):
        """Get location from lon and lat in the request parameters.

        :return: Location to be queried
        :rtype: Point
        """
        lon = self.request.GET.get('lon', None)
        lat = self.request.GET.get('lat', None)
        if lon is None or lat is None:
            return None
        return Point(x=float(lon), y=float(lat))

    def _read_netcdf_data(
            self, provider_name: str, attribute: Attribute, point: Point):
        """Read data from netcdf files from a given Point.

        :param provider_name: Provider NetCDF: CBAM/Salient
        :type provider_name: str
        :param attribute: Attribute to be queried
        :type attribute: Attribute
        :param point: Location to be queried
        :type point: Point
        :return: Dictionary of date and netcdf data
        :rtype: dict
        """
        provider = Provider.objects.get(name=provider_name)
        start_date = self._get_date_filter('start_date')
        end_date = self._get_date_filter('end_date')
        results = {}
        for filter_date in daterange_inc(start_date, end_date):
            filter_datetime = datetime.combine(filter_date, time.min)
            netcdf_file = NetCDFFile.objects.filter(
                provider=provider,
                start_date_time__gte=filter_datetime,
                end_date_time__lte=filter_datetime
            ).first()
            if netcdf_file is None:
                continue
            key = filter_date.strftime(self.date_format)
            results[key] = read_value(
                netcdf_file, attribute, point).get_value()
        return results

    @swagger_auto_schema(
        operation_id='get-measurement',
        tags=[ApiTag.Measurement],
        manual_parameters=[
            openapi.Parameter(
                'attribute', openapi.IN_QUERY,
                description='Attribute Name', type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'start_date', openapi.IN_QUERY,
                description='Start Date',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'end_date', openapi.IN_QUERY,
                description='End Date',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                'lat', openapi.IN_QUERY,
                description='Latitude',
                type=openapi.TYPE_NUMBER
            ),
            openapi.Parameter(
                'lon', openapi.IN_QUERY,
                description='Longitude',
                type=openapi.TYPE_NUMBER
            )
        ],
        responses={
            200: openapi.Schema(
                description=(
                    'Measurement data'
                ),
                type=openapi.TYPE_OBJECT,
                properties={}
            ),
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """Fetch measurement by attribute and date range filter."""
        attribute = self._get_attribute_filter()
        location = self._get_location_filter()
        data = {}
        if location is None:
            return Response(
                status=200, data=data
            )
        netcdf_provider = check_netcdf_variables(attribute)
        if netcdf_provider:
            data = self._read_netcdf_data(
                netcdf_provider, attribute, location)
        return Response(
            status=200, data=data
        )
