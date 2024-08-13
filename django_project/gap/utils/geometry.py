# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Geometry utils.
"""

from django.contrib.gis.geos import Polygon


def split_polygon_to_bbox(polygon: Polygon, size: int):
    """Split a polygon into smaller bounding box.

    :param Polygon polygon: Polygon input that will be split.
    :param int size:
        BBOX size that will be used to split the polygon in meters.
    """

    source_crs = polygon.crs

    if not source_crs:
        raise ValueError('Source CRS not provided on polygon.')

    source_srid = source_crs.srid
    used_srid = 3857
    if source_crs != used_srid:
        polygon = polygon.transform(used_srid, clone=True)

    output = []
    min_x, min_y, max_x, max_y = tuple(int(coord) for coord in polygon.extent)
    x = min_x
    while x < max_x:
        y = min_y
        while y < max_y:
            bbox = Polygon.from_bbox((x, y, x + size, y + size))
            bbox.srid = used_srid
            if polygon.overlaps(bbox) or polygon.contains(bbox):
                output.append(bbox)
            y += size
        x += size

    if source_srid != used_srid:
        return [bbox.transform(source_srid, clone=True) for bbox in output]
    else:
        return output
