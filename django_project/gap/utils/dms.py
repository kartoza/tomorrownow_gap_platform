# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DMS Utils.
"""
import re


def dms_to_decimal(
        degrees: int, minutes: int, seconds: float, direction: str
):
    """Convert DMS coordinates to decimal degrees."""
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ['S', 'W']:
        decimal = -decimal
    return decimal


def dms_string_to_lat_lon(coord_str: str) -> (float, float):
    """Change dms string to lat/lon.

    :return: tuple of (lat, lon)
    """
    pattern = re.compile(
        r'(?P<latitude>\d+°\d+\'\d+(\.\d+)?"[NS]) '
        r'(?P<longitude>\d+°\d+\'\d+(\.\d+)?"[EW])')

    match = pattern.search(coord_str)
    if not match:
        raise ValueError("Invalid format")

    latitude_str = match.group('latitude')
    longitude_str = match.group('longitude')

    def parse_dms(dms_str):
        """Extract DMS from a string."""
        degree, rest = dms_str.split('°')
        minutes, seconds = rest.split('\'')
        seconds, direction = seconds.split('"')
        return dms_to_decimal(
            int(degree), int(minutes), float(seconds), direction
        )

    latitude = parse_dms(latitude_str)
    longitude = parse_dms(longitude_str)
    return float(f"{latitude:.7f}"), float(f"{longitude:.7f}")
