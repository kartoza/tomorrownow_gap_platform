# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for Location
"""


from django.contrib.gis.db import models
from django.contrib.gis.geos import (
    Polygon, MultiPolygon,
    Point, MultiPoint
)
from django.conf import settings


def location_file_path(instance, filename):
    """Return upload path for Original file."""
    return (
        f'{settings.STORAGE_DIR_PREFIX}locations/'
        f'{str(instance.user.pk)}/{filename}'
    )


class UploadFileType:
    """Upload file types."""

    GEOJSON = 'geojson'
    SHAPEFILE = 'shapefile'
    GEOPACKAGE = 'GEOPACKAGE'


class Location(models.Model):
    """Model represents location used for API query."""

    DEFAULT_EXPIRY_IN_DAYS = 60  # 2 months
    ALLOWED_GEOMETRY_TYPES = [
        Point,
        MultiPoint,
        Polygon,
        MultiPolygon
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE
    )
    name = models.CharField(
        max_length=255
    )
    geometry = models.GeometryField(
        srid=4326
    )
    file = models.FileField(
        upload_to=location_file_path,
        null=True, blank=True
    )
    file_type = models.CharField(
        max_length=100,
        choices=(
            (UploadFileType.GEOJSON, UploadFileType.GEOJSON),
            (UploadFileType.SHAPEFILE, UploadFileType.SHAPEFILE),
            (UploadFileType.GEOPACKAGE, UploadFileType.GEOPACKAGE),
        ),
        default=UploadFileType.GEOJSON
    )
    created_on = models.DateTimeField()
    expired_on = models.DateTimeField(
        null=True,
        blank=True
    )

    class Meta:
        """Meta class for Location."""

        constraints = [
            models.UniqueConstraint(
                fields=['user', 'name'],
                name='user_locationname'
            )
        ]
