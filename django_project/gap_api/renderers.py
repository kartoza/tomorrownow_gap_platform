# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Renderer classes.
"""
from rest_framework.renderers import JSONRenderer


class GEOJSONRenderer(JSONRenderer):
    """Geojson Rendered class."""

    format = 'geojson'
