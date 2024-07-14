# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

from typing import Dict
import pytz
from datetime import date, datetime, time
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.contrib.gis.geos import Point

from gap.models import (
    Attribute,
    DatasetAttribute,
    CastType
)
from gap.utils.reader import DatasetReaderValue, BaseDatasetReader
from gap_api.serializers.common import APIErrorSerializer
from gap_api.utils.helper import ApiTag
from gap.providers import get_reader_from_dataset


class BaseMeasurementAPI(APIView):
    """Base API class for Measurement."""

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
        return Point(x=float(lon), y=float(lat), srid=4326)

    def _get_cast_type(self) -> str:
        """Get dataset cast types that the API will query.

        :return: Historical or Forecast
        :rtype: str
        """
        return CastType.HISTORICAL

    def _read_data(self, reader: BaseDatasetReader) -> DatasetReaderValue:
        """Read data from given reader.

        :param reader: NetCDF File Reader
        :type reader: BaseNetCDFReader
        :return: data value
        :rtype: DatasetReaderValue
        """
        return DatasetReaderValue({}, [])

    def get_response_data(self):
        """Read data from NetCDF File.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        attributes = self._get_attribute_filter()
        location = self._get_location_filter()
        start_dt = datetime.combine(
            self._get_date_filter('start_date'),
            time.min, tzinfo=pytz.UTC
        )
        end_dt = datetime.combine(
            self._get_date_filter('end_date'),
            time.max, tzinfo=pytz.UTC
        )
        data = {}
        if location is None:
            return data
        dataset_attributes = DatasetAttribute.objects.filter(
            attribute__in=attributes,
            dataset__type__type=self._get_cast_type()
        )
        dataset_dict: Dict[int, BaseDatasetReader] = {}
        for da in dataset_attributes:
            if da.dataset.id in dataset_dict:
                dataset_dict[da.dataset.id].add_attribute(da)
            else:
                reader = get_reader_from_dataset(da.dataset)
                dataset_dict[da.dataset.id] = reader(
                    da.dataset, [da], location, start_dt, end_dt)
        for reader in dataset_dict.values():
            values = self._read_data(reader).to_dict()
            if 'metadata' in data:
                data['metadata']['dataset'].append(
                    reader.dataset.name)
                data['metadata']['attributes'].update(
                    reader.get_attributes_metadata())
            else:
                data['metadata'] = values['metadata']
                data['metadata']['attributes'] = (
                    reader.get_attributes_metadata()
                )
            if 'data' in data:
                data['data'][reader.dataset.name] = values['data']
            else:
                data['data'] = {
                    reader.dataset.name: values['data']
                }
        return data


class HistoricalAPI(BaseMeasurementAPI):
    """Fetch historical by attribute and date range."""

    permission_classes = [IsAuthenticated]

    def _read_data(self, reader: BaseDatasetReader) -> DatasetReaderValue:
        """Read hitorical data from given reader.

        :param reader: NetCDF File Reader
        :type reader: BaseNetCDFReader
        :return: data value
        :rtype: DatasetReaderValue
        """
        reader.read_historical_data()
        return reader.get_data_values()

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
        """Fetch historical data by attributes and date range filter."""
        return Response(
            status=200,
            data=self.get_response_data()
        )


class ForecastAPI(BaseMeasurementAPI):
    """Fetch forecast by attribute and date range."""

    permission_classes = [IsAuthenticated]

    def _get_cast_type(self) -> str:
        """Get dataset cast type that the API will query.

        :return: Forecast
        :rtype: str
        """
        return CastType.FORECAST

    def _read_data(self, reader: BaseDatasetReader) -> DatasetReaderValue:
        """Read forecast data from given reader.

        :param reader: NetCDF File Reader
        :type reader: BaseNetCDFReader
        :return: data value
        :rtype: DatasetReaderValue
        """
        reader.read_forecast_data()
        return reader.get_data_values()

    @swagger_auto_schema(
        operation_id='get-forecast',
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
        """Fetch forecast by attributes and date range filter."""
        return Response(
            status=200,
            data=self.get_response_data()
        )
