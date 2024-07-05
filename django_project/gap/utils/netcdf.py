# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""


class NetCDFProvider:
    """Class contains NetCDF Provider."""

    CBAM = 'CBAM'
    SALIENT = 'Salient'


class NetCDFVariable:
    """Contains Variable from NetCDF File."""

    def __init__(self, name, desc, unit=None) -> None:
        """Initialize NetCDFVariable object."""
        self.name = name
        self.desc = desc
        self.unit = unit


CBAM_VARIABLES = {
    'total_evapotranspiration_flux': NetCDFVariable(
        'Total Evapotranspiration Flux',
        'Total Evapotranspiration flux with respect to '
        'grass cover (0000:2300)', 'mm'
    ),
    'max_total_temperature': NetCDFVariable(
        'Max Total Temperature',
        'Maximum temperature (0000:2300)', 'Deg C'
    ),
    'max_night_temperature': NetCDFVariable(
        'Max Night Temperature',
        'Maximum night-time temperature (1900:0500)', 'Deg C'
    ),
    'average_solar_irradiance': NetCDFVariable(
        'Average Solar Irradiance',
        'Average hourly solar irradiance reaching the surface (0600:1800)',
        'MJ/sqm'
    ),
    'total_solar_irradiance': NetCDFVariable(
        'Total Solar Irradiance',
        'Total solar irradiance reaching the surface (0000:2300)', 'MJ/sqm'
    ),
    'min_night_temperature': NetCDFVariable(
        'Min Night Temperature',
        'Minimum night-time temperature (1900:0500)', 'Deg C'
    ),
    'max_day_temperature': NetCDFVariable(
        'Max Day Temperature',
        'Maximum day-time temperature (0600:1800)', 'Deg C'
    ),
    'total_rainfall': NetCDFVariable(
        'Total Rainfall',
        'Total rainfall (0000:2300)', 'mm'
    ),
    'min_day_temperature': NetCDFVariable(
        'Min Day Temperature',
        'Minumum day-time temperature (0600:1800)', 'Deg C'
    ),
    'min_total_temperature': NetCDFVariable(
        'Min Total Temperature',
        'Minumum temperature (0000:2300)', 'Deg C'
    ),
}


SALIENT_VARIABLES = {
    'precip_clim': NetCDFVariable(
        'Precipitation Climatology', None, 'mm day-1'
    ),
    'temp_clim': NetCDFVariable(
        'Temperature Climatology', None, 'Deg C'
    ),
    'precip_anom': NetCDFVariable(
        'Precipitation Anomaly', None, 'mm day-1'
    ),
    'temp_anom': NetCDFVariable(
        'Temperature Anomaly', None, 'Deg C'
    ),
    'precip': NetCDFVariable(
        'Precipitation', None, 'mm day-1'
    ),
    'temp': NetCDFVariable(
        'Temperature', None, 'Deg C'
    ),
}
