# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Preferences

"""

from django.contrib.gis.db import models
from django.contrib.gis.geos import Polygon

from core.models.singleton import SingletonModel
from gap.utils.dms import dms_string_to_point

sw_point = dms_string_to_point('''-27°0'0"S 21°8'0"E''')
ne_point = dms_string_to_point('''16°0'0"N 52°0'0"E''')


def area_of_interest_default():
    """Return polygon default for area of interest."""
    sw_lon, sw_lat = sw_point.x, sw_point.y
    ne_lon, ne_lat = ne_point.x, ne_point.y

    coordinates = [
        (sw_lon, sw_lat),
        (ne_lon, sw_lat),
        (ne_lon, ne_lat),
        (sw_lon, ne_lat),
        (sw_lon, sw_lat)
    ]
    return Polygon(coordinates)


def crop_plan_config_default():
    """Return dictionary for crop plan config."""
    return {
        'lat_lon_decimal_digits': -1,
        'tz': '+02:00'  # East Africa Time
    }


class Preferences(SingletonModel):
    """Preference settings specifically for gap."""

    area_of_interest = models.PolygonField(
        srid=4326, default=area_of_interest_default
    )

    crop_plan_config = models.JSONField(
        default=crop_plan_config_default,
        blank=True
    )

    class Meta:  # noqa: D106
        verbose_name_plural = "preferences"

    def __str__(self):
        return 'Preferences'

    def collect_sort_term_forecast_tio(self):
        """Collect sort term forecast tio of area of interest."""
        from gap.utils.collector import collect_sort_term_forecast_tio
        collect_sort_term_forecast_tio(
            self.area_of_interest, 22000  # 22km size
        )
