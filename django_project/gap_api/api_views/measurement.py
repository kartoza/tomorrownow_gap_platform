# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

from typing import Dict
import pytz
import json
from datetime import date, datetime, time
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models.functions import Lower
from django.contrib.gis.geos import (
    GEOSGeometry,
    Point,
    MultiPoint,
    MultiPolygon
)

from gap.models import (
    Attribute,
    DatasetAttribute
)
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetReaderValue,
    BaseDatasetReader
)
from gap_api.serializers.common import APIErrorSerializer
from gap_api.utils.helper import ApiTag
from gap.providers import get_reader_from_dataset


class MeasurementAPI(APIView):
    """API class for measurement."""

    date_format = '%Y-%m-%d'
    permission_classes = [IsAuthenticated]
    api_parameters = [
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
            'providers', openapi.IN_QUERY,
            description='List of provider name',
            type=openapi.TYPE_STRING
        ),
    ]

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

    def _get_location_filter(self) -> DatasetReaderInput:
        """Get location from lon and lat in the request parameters.

        :return: Location to be queried
        :rtype: DatasetReaderInput
        """
        if self.request.method == 'POST':
            features = self.request.data['features']
            geom = None
            point_list = []
            for geojson in features:
                geom = GEOSGeometry(
                    json.dumps(geojson['geometry']), srid=4326
                )
                if isinstance(geom, MultiPolygon):
                    break
                point_list.append(geom[0])
            if geom is None:
                raise TypeError('Unknown geometry type!')
            if isinstance(geom, MultiPolygon):
                return DatasetReaderInput(
                    geom, LocationInputType.POLYGON)
            return DatasetReaderInput(
                MultiPoint(point_list), LocationInputType.LIST_OF_POINT)
        lon = self.request.GET.get('lon', None)
        lat = self.request.GET.get('lat', None)
        if lon is not None and lat is not None:
            return DatasetReaderInput(
                MultiPoint(
                    [Point(x=float(lon), y=float(lat), srid=4326)]
                ), LocationInputType.POINT
            )
        # (xmin, ymin, xmax, ymax)
        bbox = self.request.GET.get('bbox', None)
        if bbox is not None:
            number_list = [float(a) for a in bbox.split(',')]
            return DatasetReaderInput(
                MultiPoint([
                    Point(x=number_list[0], y=number_list[1], srid=4326),
                    Point(x=number_list[2], y=number_list[3], srid=4326)
                ]), LocationInputType.BBOX)
        return None

    def _get_provider_filter(self):
        """Get provider name filter in the request parameters.

        :return: List of provider name lowercase
        :rtype: List[str]
        """
        providers = self.request.GET.get('providers', None)
        if providers is None:
            return None
        return providers.lower().split(',')

    def _read_data(self, reader: BaseDatasetReader) -> DatasetReaderValue:
        """Read data from given reader.

        :param reader: Dataset Reader
        :type reader: BaseDatasetReader
        :return: data value
        :rtype: DatasetReaderValue
        """
        reader.read()
        return reader.get_data_values()

    def get_response_data(self):
        """Read data from dataset.

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
            attribute__in=attributes
        )
        provider_filter = self._get_provider_filter()
        if provider_filter:
            dataset_attributes = dataset_attributes.annotate(
                provider_name=Lower('dataset__provider__name')
            ).filter(
                provider_name__in=provider_filter
            )
        dataset_dict: Dict[int, BaseDatasetReader] = {}
        for da in dataset_attributes:
            if da.dataset.id in dataset_dict:
                dataset_dict[da.dataset.id].add_attribute(da)
            else:
                reader = get_reader_from_dataset(da.dataset)
                dataset_dict[da.dataset.id] = reader(
                    da.dataset, [da], location, start_dt, end_dt)
        data = {
            'metadata': {
                'start_date': start_dt.isoformat(timespec='seconds'),
                'end_date': end_dt.isoformat(timespec='seconds'),
                'dataset': []
            },
            'results': []
        }
        for reader in dataset_dict.values():
            data['metadata']['dataset'].append({
                'provider': reader.dataset.provider.name,
                'attributes': reader.get_attributes_metadata()
            })
            values = self._read_data(reader).to_dict()
            if values:
                data['results'].append(values)
        return data

    @swagger_auto_schema(
        operation_id='get-measurement',
        tags=[ApiTag.Measurement],
        manual_parameters=[
            *api_parameters,
            openapi.Parameter(
                'lat', openapi.IN_QUERY,
                description='Latitude',
                type=openapi.TYPE_NUMBER
            ),
            openapi.Parameter(
                'lon', openapi.IN_QUERY,
                description='Longitude',
                type=openapi.TYPE_NUMBER
            ),
            openapi.Parameter(
                'bbox', openapi.IN_QUERY,
                description='Bounding box: xmin, ymin, xmax, ymax',
                type=openapi.TYPE_STRING
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
        """Fetch measurement data by attributes and date range filter."""
        return Response(
            status=200,
            data=self.get_response_data()
        )

    @swagger_auto_schema(
        operation_id='get-measurement-by-polygon',
        tags=[ApiTag.Measurement],
        manual_parameters=[
            *api_parameters
        ],
        request_body=openapi.Schema(
            description='Polygon (SRID 4326) in geojson format',
            type=openapi.TYPE_STRING
        ),
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
    def post(self, request, *args, **kwargs):
        """Fetch measurement data by polygon."""
        return Response(
            status=200,
            data=self.get_response_data()
        )
