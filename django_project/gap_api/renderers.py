# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Renderer classes.
"""
from rest_framework.renderers import JSONRenderer
from rest_framework_csv.renderers import CSVRenderer


class GEOJSONRenderer(JSONRenderer):
    """Geojson Rendered class."""

    format = 'geojson'


class CSVDynamicHeaderRenderer(CSVRenderer):
    """CSV Rendered class."""

    def render(
            self, data, media_type=None, renderer_context={}, writer_opts=None
    ):
        """Renders serialized *data* into CSV. For a dictionary"""
        if not self.header:
            self.header = data[0].keys()
        return super().render(data, media_type, renderer_context, writer_opts)
