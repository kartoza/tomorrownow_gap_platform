# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Measurement APIs
"""

from datetime import date, datetime, time
from typing import Dict

import pytz
from django.contrib.gis.geos import (
    Point
)
from django.db.models.functions import Lower
from django.db.utils import ProgrammingError
from django.http import StreamingHttpResponse
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from gap.models import (
    Dataset,
    DatasetObservationType,
    Attribute,
    DatasetAttribute
)
from gap.providers import get_reader_from_dataset
from gap.utils.reader import (
    LocationInputType,
    DatasetReaderInput,
    DatasetReaderValue,
    BaseDatasetReader,
    DatasetReaderOutputType
)
from gap_api.models import DatasetTypeAPIConfig, Location
from gap_api.serializers.common import APIErrorSerializer
from gap_api.utils.helper import ApiTag
from gap_api.mixins import GAPAPILoggingMixin


def attribute_list():
    """Attribute list."""
    try:
        return list(
            Attribute.objects.all().values_list(
                'variable_name', flat=True
            ).order_by('variable_name')
        )
    except ProgrammingError:
        pass
    except RuntimeError:
        pass


def default_attribute_list():
    """Attribute list."""
    try:
        first = Attribute.objects.all().order_by('variable_name').first()
        if first:
            return Attribute.objects.all().order_by(
                'variable_name'
            ).first().variable_name
        else:
            return ''
    except ProgrammingError:
        pass
    except RuntimeError:
        pass


class MeasurementAPI(GAPAPILoggingMixin, APIView):
    """API class for measurement."""

    date_format = '%Y-%m-%d'
    time_format = '%H:%M:%S'
    permission_classes = [IsAuthenticated]
    api_parameters = [
        openapi.Parameter(
            'product', openapi.IN_QUERY,
            required=True,
            description='Product type',
            type=openapi.TYPE_STRING,
            enum=[
                'cbam_historical_analysis',
                'arable_ground_observation',
                'disdrometer_ground_observation',
                'tahmo_ground_observation',
                'windborne_radiosonde_observation',
                'cbam_shortterm_forecast',
                'salient_seasonal_forecast'
            ],
            default='cbam_historical_analysis'
        ),
        openapi.Parameter(
            'attributes',
            openapi.IN_QUERY,
            required=True,
            description='List of attribute name',
            type=openapi.TYPE_ARRAY,
            items=openapi.Items(
                type=openapi.TYPE_STRING,
                enum=attribute_list(),
                default=default_attribute_list()
            )
        ),
        openapi.Parameter(
            'start_date', openapi.IN_QUERY,
            required=True,
            description='Start Date (YYYY-MM-DD)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'start_time', openapi.IN_QUERY,
            description='Start Time - UTC (HH:MM:SS)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'end_date', openapi.IN_QUERY,
            required=True,
            description='End Date (YYYY-MM-DD)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'end_time', openapi.IN_QUERY,
            description='End Time - UTC (HH:MM:SS)',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'output_type', openapi.IN_QUERY,
            required=True,
            description='Returned format',
            type=openapi.TYPE_STRING,
            enum=[
                DatasetReaderOutputType.JSON,
                DatasetReaderOutputType.NETCDF,
                DatasetReaderOutputType.CSV,
                DatasetReaderOutputType.ASCII
            ],
            default=DatasetReaderOutputType.JSON
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
        ),
        openapi.Parameter(
            'bbox', openapi.IN_QUERY,
            description='Bounding box: xmin, ymin, xmax, ymax',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'location_name', openapi.IN_QUERY,
            description='User location name that has been uploaded',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'altitudes', openapi.IN_QUERY,
            description='2 value of altitudes: alt_min, alt_max',
            type=openapi.TYPE_STRING
        )
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

    def _get_time_filter(self, attr_name, default):
        """Get time object from filter (start_time/end_time).

        :param attr_name: request parameter name
        :type attr_name: str
        :param default: Time.min or Time.max
        :type default: time
        :return: Time object
        :rtype: time
        """
        time_str = self.request.GET.get(attr_name, None)
        return (
            default if time_str is None else
            datetime.strptime(
                time_str, self.time_format
            ).replace(tzinfo=None).time()
        )

    def _get_location_filter(self) -> DatasetReaderInput:
        """Get location from lon and lat in the request parameters.

        :return: Location to be queried
        :rtype: DatasetReaderInput
        """
        lon = self.request.GET.get('lon', None)
        lat = self.request.GET.get('lat', None)
        if lon is not None and lat is not None:
            return DatasetReaderInput.from_point(
                Point(x=float(lon), y=float(lat), srid=4326))

        # (xmin, ymin, xmax, ymax)
        bbox = self.request.GET.get('bbox', None)
        if bbox is not None:
            number_list = [float(a) for a in bbox.split(',')]
            return DatasetReaderInput.from_bbox(number_list)

        # location_name
        location_name = self.request.GET.get('location_name', None)
        if location_name is not None:
            location = Location.objects.filter(
                user=self.request.user,
                name=location_name
            ).first()
            if location:
                return location.to_input()

        return None

    def _get_altitudes_filter(self):
        """Get list of altitudes in the query parameter.

        :return: altitudes list
        :rtype: (int, int)
        """
        altitudes_str = self.request.GET.get('altitudes', None)
        if altitudes_str is None:
            return None, None
        try:
            altitudes = [
                float(altitude) for altitude in altitudes_str.split(',')
            ]
        except ValueError:
            raise ValidationError('altitudes not a float')
        if len(altitudes) != 2:
            raise ValidationError(
                'altitudes needs to be a comma-separated list, '
                'contains 2 number'
            )
        if altitudes[0] > altitudes[1]:
            raise ValidationError(
                'First altitude needs to be greater than second altitude'
            )
        return altitudes[0], altitudes[1]

    def _get_product_filter(self):
        """Get product name filter in the request parameters.

        :return: List of product type lowercase
        :rtype: List[str]
        """
        product = self.request.GET.get('product', None)
        if product is None:
            return ['cbam_historical_analysis']
        return [product.lower()]

    def _get_format_filter(self):
        """Get format filter in the request parameters.

        :return: List of product type lowercase
        :rtype: List[str]
        """
        product = self.request.GET.get(
            'output_type', DatasetReaderOutputType.JSON)
        return product.lower()

    def _read_data(self, reader: BaseDatasetReader) -> DatasetReaderValue:
        reader.read()
        return reader.get_data_values()

    def _read_data_as_json(
            self, reader_dict: Dict[int, BaseDatasetReader],
            start_dt: datetime, end_dt: datetime) -> DatasetReaderValue:
        """Read data from given reader.

        :param reader: Dataset Reader
        :type reader: BaseDatasetReader
        :return: data value
        :rtype: DatasetReaderValue
        """
        data = {
            'metadata': {
                'start_date': start_dt.isoformat(timespec='seconds'),
                'end_date': end_dt.isoformat(timespec='seconds'),
                'dataset': []
            },
            'results': []
        }
        for reader in reader_dict.values():
            reader_value = self._read_data(reader)
            if reader_value.is_empty():
                return None
            values = reader_value.to_json()
            if values:
                data['metadata']['dataset'].append({
                    'provider': reader.dataset.provider.name,
                    'attributes': reader.get_attributes_metadata()
                })
                data['results'].append(values)
        return data

    def _read_data_as_netcdf(
            self, reader_dict: Dict[int, BaseDatasetReader],
            start_dt: datetime, end_dt: datetime) -> Response:
        reader: BaseDatasetReader = list(reader_dict.values())[0]
        reader_value = self._read_data(reader)
        if reader_value.is_empty():
            return None
        response = StreamingHttpResponse(
            reader_value.to_netcdf_stream(),
            content_type='application/x-netcdf'
        )
        response['Content-Disposition'] = 'attachment; filename="data.nc"'

        return response

    def _read_data_as_csv(
            self, reader_dict: Dict[int, BaseDatasetReader],
            start_dt: datetime, end_dt: datetime) -> Response:
        reader: BaseDatasetReader = list(reader_dict.values())[0]
        reader_value = self._read_data(reader)
        if reader_value.is_empty():
            return None
        response = StreamingHttpResponse(
            reader_value.to_csv_stream(), content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="data.csv"'
        return response

    def _read_data_as_ascii(
            self, reader_dict: Dict[int, BaseDatasetReader],
            start_dt: datetime, end_dt: datetime) -> Response:
        reader: BaseDatasetReader = list(reader_dict.values())[0]
        reader_value = self._read_data(reader)
        if reader_value.is_empty():
            return None
        response = StreamingHttpResponse(
            reader_value.to_csv_stream('.txt', '\t'),
            content_type='text/ascii')
        response['Content-Disposition'] = 'attachment; filename="data.txt"'
        return response

    def validate_output_format(
            self, dataset: Dataset, product_type: str,
            location: DatasetReaderInput, output_format):
        """Validate output format.

        :param dataset: dataset to read
        :type dataset: Dataset
        :param product_type: product type in request
        :type product_type: str
        :param location: location input filter
        :type location: DatasetReaderInput
        :param output_format: output type/format
        :type output_format: str
        :raises ValidationError: invalid request
        """
        if output_format == DatasetReaderOutputType.JSON:
            if location.type != LocationInputType.POINT:
                raise ValidationError({
                    'Invalid Request Parameter': (
                        'Output format json is only available '
                        'for single point query!'
                    )
                })
        elif output_format == DatasetReaderOutputType.NETCDF:
            if (
                dataset.observation_type ==
                DatasetObservationType.UPPER_AIR_OBSERVATION
            ):
                raise ValidationError({
                    'Invalid Request Parameter': (
                        'Output format NETCDF is not available '
                        f'for {product_type}!'
                    )
                })

    def validate_dataset_attributes(self, dataset_attributes, output_format):
        """Validate the attributes for the query.

        :param dataset_attributes: dataset attribute list
        :type dataset_attributes: List[DatasetAttribute]
        :param output_format: output type/format
        :type output_format: str
        :raises ValidationError: invalid request
        """
        if len(dataset_attributes) == 0:
            raise ValidationError({
                'Invalid Request Parameter': (
                    'No matching attribute found!'
                )
            })

        if output_format == DatasetReaderOutputType.CSV:
            non_ensemble_count = dataset_attributes.filter(
                ensembles=False
            ).count()
            ensemble_count = dataset_attributes.filter(
                ensembles=True
            ).count()
            if ensemble_count > 0 and non_ensemble_count > 0:
                raise ValidationError({
                    'Invalid Request Parameter': (
                        'Attribute with ensemble cannot be mixed '
                        'with non-ensemble'
                    )
                })

    def validate_date_range(self, product_filter, start_dt, end_dt):
        """Validate maximum date range based on product filter.

        :param product_filter: list of product type
        :type product_filter: List[str]
        :param start_dt: start date query
        :type start_dt: datetime
        :param end_dt: end date query
        :type end_dt: datetime
        """
        configs = DatasetTypeAPIConfig.objects.filter(
            type__variable_name__in=product_filter
        )
        diff = end_dt - start_dt

        for config in configs:
            if config.max_daterange == -1:
                continue

            if diff.days + 1 > config.max_daterange:
                raise ValidationError({
                    'Invalid Request Parameter': (
                        f'Maximum date range is {config.max_daterange}'
                    )
                })

    def get_response_data(self) -> Response:
        """Read data from dataset.

        :return: Dictionary of metadata and data
        :rtype: dict
        """
        attributes = self._get_attribute_filter()
        location = self._get_location_filter()
        min_altitudes, max_altitudes = self._get_altitudes_filter()
        start_dt = datetime.combine(
            self._get_date_filter('start_date'),
            self._get_time_filter('start_time', time.min), tzinfo=pytz.UTC
        )
        end_dt = datetime.combine(
            self._get_date_filter('end_date'),
            self._get_time_filter('end_time', time.max), tzinfo=pytz.UTC
        )
        output_format = self._get_format_filter()
        if location is None:
            return Response(
                status=400,
                data={
                    'Invalid Request Parameter': (
                        'Missing location input parameter!'
                    )
                }
            )

        dataset_attributes = DatasetAttribute.objects.select_related(
            'dataset'
        ).filter(
            attribute__in=attributes,
            dataset__is_internal_use=False,
            attribute__is_active=True
        )
        product_filter = self._get_product_filter()
        dataset_attributes = dataset_attributes.annotate(
            product_name=Lower('dataset__type__variable_name')
        ).filter(
            product_name__in=product_filter
        ).order_by('dataset__type__variable_name')

        # validate empty dataset_attributes
        self.validate_dataset_attributes(dataset_attributes, output_format)

        # validate output type
        self.validate_output_format(
            dataset_attributes.first().dataset, product_filter, location,
            output_format)

        # validate date range
        self.validate_date_range(product_filter, start_dt, end_dt)

        dataset_dict: Dict[int, BaseDatasetReader] = {}
        for da in dataset_attributes:
            if da.dataset.id in dataset_dict:
                dataset_dict[da.dataset.id].add_attribute(da)
            else:
                try:
                    reader = get_reader_from_dataset(da.dataset)
                    dataset_dict[da.dataset.id] = reader(
                        da.dataset, [da], location, start_dt, end_dt,
                        altitudes=(min_altitudes, max_altitudes),
                    )
                except TypeError as e:
                    print(e)
                    pass

        response = None
        if output_format == DatasetReaderOutputType.JSON:
            data_value = self._read_data_as_json(
                dataset_dict, start_dt, end_dt)
            if data_value:
                response = Response(
                    status=200,
                    data=data_value
                )
        elif output_format == DatasetReaderOutputType.NETCDF:
            response = self._read_data_as_netcdf(
                dataset_dict, start_dt, end_dt)
        elif output_format == DatasetReaderOutputType.CSV:
            response = self._read_data_as_csv(dataset_dict, start_dt, end_dt)
        elif output_format == DatasetReaderOutputType.ASCII:
            response = self._read_data_as_ascii(dataset_dict, start_dt, end_dt)
        else:
            raise ValidationError({
                'Invalid Request Parameter': (
                    f'Incorrect output_type value: {output_format}'
                )
            })

        if response is None:
            return Response(
                status=404,
                data={
                    'detail': 'No weather data is found for given queries.'
                }
            )
        return response

    @swagger_auto_schema(
        operation_id='get-measurement',
        operation_description=(
            "Fetch weather data using either a single point or bounding box "
            "and attribute filters."
        ),
        tags=[ApiTag.Measurement],
        manual_parameters=[
            *api_parameters
        ],
        responses={
            200: openapi.Schema(
                description=(
                    'Weather data'
                ),
                type=openapi.TYPE_OBJECT,
                properties={}
            ),
            400: APIErrorSerializer
        }
    )
    def get(self, request, *args, **kwargs):
        """Fetch weather data by a single point or bounding box."""
        return self.get_response_data()
