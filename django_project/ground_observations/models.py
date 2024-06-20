from django.db import models

# Create your models here.

from django.contrib.gis.db import models as gis_models
import uuid

class Provider(models.Model):
    """
    Model representing a data provider.
    
    Attributes:
        provider_id (UUID): Unique identifier for the provider, generated automatically.
        name (str): Name of the provider.
        description (str): Description of the provider.
    """
    provider_id = models.UUIDField(unique=True, default=uuid.uuid4, editable=False)
    name = models.TextField()
    description = models.TextField()

    def __str__(self):
        return self.name

class Attribute(models.Model):
    """
    Model representing an attribute of a measurement.
    
    Attributes:
        attribute_id (UUID): Unique identifier for the attribute, generated automatically.
        name (str): Name of the attribute.
        description (str): Description of the attribute.
    """
    attribute_id = models.UUIDField(unique=True, default=uuid.uuid4, editable=False)
    name = models.TextField()
    description = models.TextField()

    def __str__(self):
        return self.name

class Country(models.Model):
    """
    Model representing a country.
    
    Attributes:
        name (str): Name of the country.
        country_ISO_A3 (str): ISO A3 country code, unique.
        geometry (Polygon): Polygon geometry representing the country boundaries.
    """
    name = models.TextField()
    country_ISO_A3 = models.TextField(unique=True)
    geometry = gis_models.PolygonField(srid=4326)

    def __str__(self):
        return self.name

class Station(models.Model):
    """
    Model representing a ground observation station.
    
    Attributes:
        station_id (UUID): Unique identifier for the station, generated automatically.
        name (str): Name of the station.
        country (ForeignKey): Foreign key referencing the Country model based on country_ISO_A3.
        geometry (Point): Point geometry representing the location of the station.
        provider_id (ForeignKey): Foreign key referencing the Provider model based on provider_id.
    """
    station_id = models.UUIDField(unique=True, default=uuid.uuid4, editable=False)
    name = models.TextField()
    country = models.ForeignKey(Country, on_delete=models.CASCADE, to_field='country_ISO_A3', db_column='country_ISO_A3')
    geometry = gis_models.PointField(srid=4326)
    provider_id = models.ForeignKey(Provider, on_delete=models.CASCADE, to_field='provider_id')

    def __str__(self):
        return self.name

class Measurement(models.Model):
    """
    Model representing a measurement taken at a station.
    
    Attributes:
        date (date): Date of the measurement.
        value (float): Value of the measurement.
        station_id (ForeignKey): Foreign key referencing the Station model based on station_id.
        attribute_id (ForeignKey): Foreign key referencing the Attribute model based on attribute_id.
    """
    date = models.DateField()
    value = models.FloatField()
    station_id = models.ForeignKey(Station, on_delete=models.CASCADE, to_field='station_id')
    attribute_id = models.ForeignKey(Attribute, on_delete=models.CASCADE, to_field='attribute_id')

    def __str__(self):
        return f'{self.date} - {self.value}'
