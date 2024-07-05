# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

import os
from typing import List
from datetime import date, datetime, time, timedelta
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.contrib.gis.geos import Point
import xarray as xr
import fsspec

from gap.models import (
    Dataset,
    Attribute,
    DatasetAttribute,
    NetCDFFile,
    DatasetStore,
    DatasetType
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
        """Get list of attributes in the query parameter.

        :return: attribute list
        :rtype: List[Attribute]
        """
        attributes_str = self.request.GET.get('attributes')
        attributes_str = attributes_str.split(',')
        return Attribute.objects.filter(variable_name__in=attributes_str)

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

    def _read_netcdf_historical_data(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            point: Point):
        """Read NetCDF historical data.

        :param dataset: NetCDF Dataset
        :type dataset: Dataset
        :param attributes: list of attributes
        :type attributes: List[DatasetAttribute]
        :param point: Location to be queried
        :type point: Point
        :return: Dictionary of Metadata and Data
        :rtype: Dict
        """
        bucket_name = os.environ.get('S3_AWS_BUCKET_NAME')
        endpoint_url = os.environ.get('AWS_ENDPOINT_URL')
        fs = fsspec.filesystem(
            's3', client_kwargs=dict(endpoint_url=endpoint_url)
        )
        start_date = self._get_date_filter('start_date')
        end_date = self._get_date_filter('end_date')
        variables = [a.source for a in attributes]
        results = []
        for filter_date in daterange_inc(start_date, end_date):
            filter_datetime = datetime.combine(filter_date, time.min)
            netcdf_file = NetCDFFile.objects.filter(
                dataset=dataset,
                start_date_time__gte=filter_datetime,
                end_date_time__lte=filter_datetime
            ).first()
            if netcdf_file is None:
                continue
            netcdf_url = f's3://{bucket_name}/{netcdf_file.name}'
            ds = xr.open_dataset(fs.open(netcdf_url))
            val = ds[variables].sel(
                lat=point.y, lon=point.x, method='nearest')
            value_data = {}
            for attribute in attributes:
                value_data[attribute.attribute.variable_name] = (
                    val[attribute.source].values[0]
                )
            results.append({
                'datetime': filter_date.strftime(self.date_format),
                'values': value_data
            })
        return {
            'metadata': {
                'dataset': dataset.name
            },
            'data': results
        }

    @swagger_auto_schema(
        operation_id='get-measurement',
        tags=[ApiTag.Measurement],
        manual_parameters=[
            openapi.Parameter(
                'attributes', openapi.IN_QUERY,
                description='List of attribute name', type=openapi.TYPE_STRING
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
        """Fetch measurement by attributes and date range filter."""
        attributes = self._get_attribute_filter()
        location = self._get_location_filter()
        data = {}
        if location is None:
            return Response(
                status=200, data=data
            )
        dataset_attributes = DatasetAttribute.objects.filter(
            attribute__in=attributes
        )
        dataset_dict = {}
        for da in dataset_attributes:
            if da.dataset.id in dataset_dict:
                dataset_dict[da.dataset.id]['attributes'].append(da)
            else:
                dataset_dict[da.dataset.id] = {
                    'dataset': da.dataset,
                    'attributes': [da]
                }
        for ds_mapping in dataset_dict.values():
            dataset: Dataset = ds_mapping['dataset']
            if dataset.store_type == DatasetStore.NETCDF:
                if dataset.type == DatasetType.CLIMATE_REANALYSIS:
                    data = self._read_netcdf_historical_data(
                        dataset, ds_mapping['attributes'], location)
        # TODO: need to merge data between different dataset
        return Response(
            status=200, data=data
        )
