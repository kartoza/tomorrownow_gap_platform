# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Location APIs
"""

import os
import json
import fiona
import logging
from typing import Tuple
from django.utils import timezone
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.contrib.gis.geos import GEOSGeometry, MultiPoint, MultiPolygon
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
import fiona.model
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser

from gap_api.models.location import Location, UploadFileType
from gap_api.serializers.common import APIErrorSerializer
from gap_api.serializers.location import LocationSerializer
from gap_api.utils.helper import ApiTag
from gap_api.mixins import GAPAPILoggingMixin
from gap_api.utils.fiona import (
    validate_shapefile_zip,
    validate_collection_crs,
    delete_tmp_shapefile,
    open_fiona_collection
)


logger = logging.getLogger(__name__)


class LocationAPI(GAPAPILoggingMixin, APIView):
    """API class for uploading location."""

    permission_classes = [IsAuthenticated]
    parser_classes = (MultiPartParser,)
    api_parameters = [
        openapi.Parameter(
            'location_name', openapi.IN_QUERY,
            required=True,
            description='Location Name',
            type=openapi.TYPE_STRING
        ),
        openapi.Parameter(
            'file', openapi.IN_FORM,
            description=(
                'Geometry data (SRID 4326) in one of the format: '
                'geojson, shapefile, GPKG'
            ),
            type=openapi.TYPE_FILE
        )
    ]

    def _check_file_type(self, filename: str) -> str:
        """Check file type from upload filename.

        :param filename: filename of uploaded file
        :type filename: str
        :return: file type
        :rtype: str
        """
        if (filename.lower().endswith('.geojson') or
                filename.lower().endswith('.json')):
            return UploadFileType.GEOJSON
        elif filename.lower().endswith('.zip'):
            return UploadFileType.SHAPEFILE
        elif filename.lower().endswith('.gpkg'):
            return UploadFileType.GEOPACKAGE
        return ''

    def _check_shapefile_zip(self, file_obj: any) -> str:
        """Validate if zip shapefile has complete files.

        :param file_obj: file object
        :type file_obj: file
        :return: list of error
        :rtype: str
        """
        _, error = validate_shapefile_zip(file_obj)
        if error:
            return ('Missing required file(s) inside zip file: \n- ' +
                    '\n- '.join(error)
                    )
        return ''

    def _remove_temp_files(self, file_obj_list: list) -> None:
        """Remove temporary files.

        :param file_obj: list temporary files
        :type file_obj: list
        """
        for file_obj in file_obj_list:
            if isinstance(file_obj, TemporaryUploadedFile):
                if os.path.exists(file_obj.temporary_file_path()):
                    os.remove(file_obj.temporary_file_path())
            elif isinstance(file_obj, str):
                delete_tmp_shapefile(file_obj)

    def _build_geom_object(self, fiona_geom) -> GEOSGeometry:
        """Build geometry object from string.

        :param fiona_geom: geometry fiona object
        :type fiona_geom: Fiona.Geometry
        :return: geometry object if valid one
        :rtype: GEOSGeometry
        """
        geom = None
        try:
            geom = GEOSGeometry(fiona_geom)
        except Exception as ex:
            logger.error(ex)
        return geom

    def _read_geom(self, collection) -> Tuple[GEOSGeometry, str]:
        """Read geometry object from uploaded file.

        If there are multiple features of Point/Polygon, this method will
        return them as MultiPoint/MultiPolygon
        :param collection: fiona collection object
        :type collection: Collection
        :return: geometry object and error
        :rtype: Tuple[GEOSGeometry, str]
        """
        error = None
        geometries = []
        geom_type = ''
        for feature_idx, feature in enumerate(collection):
            geom_type = feature['geometry']['type']
            if geom_type not in Location.ALLOWED_GEOMETRY_TYPES:
                error = f'Geometry type {geom_type} is not allowed!'
                break

            geom_str = json.dumps(
                feature['geometry'], cls=fiona.model.ObjectEncoder)
            geom = self._build_geom_object(geom_str)
            if geom is None:
                error = (
                    f'Unable to parse geometry on feature {feature_idx}!'
                )
                break

            geometries.append(geom)
            if geom_type not in ['Point', 'Polygon']:
                # for geom collection, take the first feature
                break

        if error:
            return None, error

        if geom_type == 'Point':
            return (
                MultiPoint(geometries) if len(geometries) > 1 else
                geometries[0], None
            )
        elif geom_type == 'Polygon':
            return (
                MultiPolygon(geometries) if len(geometries) > 1 else
                geometries[0], None
            )
        return geometries[0], None

    def _on_validation_error(self, error: str, file_obj_list: list):
        """Handle when there is error on validation."""
        self._remove_temp_files(file_obj_list)
        raise ValidationError({
            'Invalid Request Parameter': error
        })

    @swagger_auto_schema(
        operation_id='upload-location',
        operation_description=(
            "Upload polygon/points for querying weather data."
        ),
        tags=[ApiTag.Location],
        manual_parameters=[
            *api_parameters
        ],
        responses={
            200: LocationSerializer,
            400: APIErrorSerializer
        }
    )
    def post(self, request, *args, **kwargs):
        """Post method for uploading location."""
        name = request.GET.get('location_name', None)
        if not name:
            raise ValidationError({
                'Invalid Request Parameter': (
                    'location_name is mandatory!'
                )
            })

        file_obj = request.data['file']
        tmp_file_obj_list = [file_obj]

        # validate uploaded file
        file_type = self._check_file_type(file_obj.name)
        if file_type == '':
            self._on_validation_error(
                'Unrecognized file type!', tmp_file_obj_list)

        if file_type == UploadFileType.SHAPEFILE:
            validate_shp_file = self._check_shapefile_zip(file_obj)
            if validate_shp_file != '':
                self._on_validation_error(
                    validate_shp_file, tmp_file_obj_list)

        # open fiona collection
        collection = open_fiona_collection(file_obj, file_type)
        tmp_file_obj_list.append(collection.path)

        is_valid_crs, crs = validate_collection_crs(collection)
        if not is_valid_crs:
            self._on_validation_error(
                f'Incorrect CRS type: {crs}!', tmp_file_obj_list)

        # read geometry
        geometry, error = self._read_geom(collection)
        if error:
            self._on_validation_error(
                error, tmp_file_obj_list)

        # create location object
        location, _ = Location.objects.update_or_create(
            user=request.user,
            name=name,
            defaults={
                'created_on': timezone.now(),
                'geometry': geometry
            }
        )

        # close collection
        collection.close()

        # remove temporary uploaded file if any
        self._remove_temp_files(tmp_file_obj_list)

        return Response(
            status=200, data=LocationSerializer(location).data
        )
