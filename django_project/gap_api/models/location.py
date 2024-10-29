# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for Location
"""

from datetime import timedelta
from django.contrib.gis.db import models
from django.contrib.gis.geos import (
    Polygon, MultiPolygon,
    Point, MultiPoint
)
from django.conf import settings
from django.utils import timezone


class UploadFileType:
    """Upload file types."""

    GEOJSON = 'geojson'
    SHAPEFILE = 'shapefile'
    GEOPACKAGE = 'geopackage'


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

    def save(self, *args, **kwargs):
        """Override location save."""
        if not self.pk:
            self.expired_on = (
                timezone.now() +
                timedelta(days=Location.DEFAULT_EXPIRY_IN_DAYS)
            )
        super(Location, self).save(*args, **kwargs)

    def to_input(self):
        """Return location object as DatasetReaderInput.

        :return: DatasetReaderInput object
        :rtype: DatasetReaderInput
        """
        from gap.utils.reader import DatasetReaderInput, LocationInputType
        location_type = LocationInputType.map_from_geom_typeid(
            self.geometry.geom_typeid)

        return DatasetReaderInput(self.geometry, location_type)
