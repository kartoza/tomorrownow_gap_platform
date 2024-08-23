# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

import json
import uuid
from datetime import date

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.db import models
from django.core.files.base import ContentFile
from django.core.mail import EmailMessage
from django.utils import timezone

from core.group_email_receiver import crop_plan_receiver
from core.models.common import Definition
from gap.models import Farm
from gap.models.lookup import RainfallClassification
from gap.models.measurement import DatasetAttribute
from spw.models import SPWOutput

User = get_user_model()


def ingestor_file_path(instance, filename):
    """Return upload path for Ingestor files."""
    return f'{settings.STORAGE_DIR_PREFIX}crop-insight/{filename}'


class Crop(Definition):
    """Model representing crop."""

    pass


class Pest(Definition):
    """Model representing pest."""

    pass


class FarmShortTermForecast(models.Model):
    """Model representing Farm Short-Term Weather Forecast Table."""

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    forecast_date = models.DateField(
        default=timezone.now,
        help_text='Date when the forecast is made'
    )

    class Meta:  # noqa: D106
        ordering = ['-forecast_date']

    def __str__(self):
        return f'{self.farm.__str__()} - {self.forecast_date}'


class FarmShortTermForecastData(models.Model):
    """Model representing Farm Short-Term Weather Forecast Table data."""

    forecast = models.ForeignKey(
        FarmShortTermForecast, on_delete=models.CASCADE
    )
    dataset_attribute = models.ForeignKey(
        DatasetAttribute, on_delete=models.CASCADE,
        help_text='Forecast attribute'
    )
    value_date = models.DateField(
        help_text='Date when the value is occurred on forcast'
    )
    value = models.FloatField(
        help_text='The value of the forecast attribute'
    )

    class Meta:  # noqa: D106
        ordering = ['dataset_attribute', '-value_date']


class FarmProbabilisticWeatherForcast(models.Model):
    """Model representing Farm Probabilistic S2S Weather Forecast Table.

    Attributes:
        forecast_period (str):
            Forecast period (
                e.g., '2 weeks', 'week 2-4', 'week 4-8', 'week 8-12'
            ).
        temperature_10th_percentile (Float):
            10th percentile of temperature forecast
        temperature_50th_percentile (Float):
            50th percentile (median) of temperature forecast
        temperature_90th_percentile (Float):
            90th percentile of temperature forecast
        precipitation_10th_percentile (Float):
            10th percentile of precipitation forecast
        precipitation_50th_percentile (Float):
            50th percentile (median) of precipitation forecast
        precipitation_90th_percentile (Float):
            90th percentile of precipitation forecast
        other_parameters (dict):
            JSON object to store additional probabilistic forecast parameters
            (e.g., humidity, wind speed)
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    forecast_date = models.DateField(
        default=timezone.now,
        help_text='Date when the forecast is made'
    )
    forecast_period = models.CharField(
        max_length=512,
        help_text=(
            "Forecast period "
            "(e.g., '2 weeks', 'week 2-4', 'week 4-8', 'week 8-12')"
        )
    )
    temperature_10th_percentile = models.FloatField(
        help_text='10th percentile of temperature forecast'
    )
    temperature_50th_percentile = models.FloatField(
        help_text='50th percentile (median) of temperature forecast'
    )
    temperature_90th_percentile = models.FloatField(
        help_text='90th percentile of temperature forecast'
    )
    precipitation_10th_percentile = models.FloatField(
        help_text='10th percentile of precipitation forecast'
    )
    precipitation_50th_percentile = models.FloatField(
        help_text='50th percentile (median) of precipitation forecast'
    )
    precipitation_90th_percentile = models.FloatField(
        help_text='90th percentile of precipitation forecast'
    )
    other_parameters = models.JSONField(
        null=True, blank=True,
        help_text=(
            'JSON object to store additional probabilistic forecast '
            'parameters (e.g., humidity, wind speed)'
        )
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.forecast_date}'

    class Meta:  # noqa: D106
        ordering = ['-forecast_date']


class FarmSuitablePlantingWindowSignal(models.Model):
    """Model representing Farm Suitable Planting Window Signal.

    Attributes:
        signal (str): Suitable planting window signal.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    generated_date = models.DateField(
        default=timezone.now,
        help_text='Date when the signal was generated'
    )
    signal = models.CharField(
        max_length=512,
        help_text='Signal value of Suitable Planting Window l.'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.generated_date}'

    class Meta:  # noqa: D106
        ordering = ['-generated_date']


class FarmPlantingWindowTable(models.Model):
    """Model representing Farm Planting Window Table.

    Attributes:
        recommendation_date (Date): Recommended planting date.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    recommendation_date = models.DateField(
        default=timezone.now,
        help_text='Date when the recommendation was made'
    )
    recommended_date = models.DateField(
        help_text='Recommended planting date'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.recommendation_date}'

    class Meta:  # noqa: D106
        ordering = ['-recommendation_date']


class FarmPestManagement(models.Model):
    """Model representing Farm Pest Management.

    Attributes:
        spray_recommendation (str): Recommended pest spray action.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    recommendation_date = models.DateField(
        default=timezone.now,
        help_text='Date when the recommendation was made'
    )
    spray_recommendation = models.CharField(
        max_length=512,
        help_text='Recommended pest spray action'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.recommendation_date}'

    class Meta:  # noqa: D106
        ordering = ['-recommendation_date']


class FarmCropVariety(models.Model):
    """Model representing Farm Crop Variety.

    Attributes:
        recommended_crop (str): Recommended crop variety.
    """

    farm = models.ForeignKey(
        Farm, on_delete=models.CASCADE
    )
    recommendation_date = models.DateField(
        default=timezone.now,
        help_text='Date when the recommendation was made'
    )
    recommended_crop = models.ForeignKey(
        Crop, on_delete=models.CASCADE,
        help_text='Recommended crop variety'
    )

    def __str__(self):
        return f'{self.farm.__str__()} - {self.recommendation_date}'

    class Meta:  # noqa: D106
        ordering = ['-recommendation_date']


class CropPlanData:
    """The report model for the Insight Request Report."""

    @staticmethod
    def default_fields():
        """Return shortterm default fields."""
        from gap.providers.tio import tomorrowio_shortterm_forcast_dataset

        dataset = tomorrowio_shortterm_forcast_dataset()
        forecast_fields = list(
            DatasetAttribute.objects.filter(
                dataset=dataset
            ).values_list(
                'source', flat=True
            )
        )
        if 'rainAccumulationSum' in forecast_fields:
            forecast_fields.append('rainAccumulationType')
        return forecast_fields

    def __init__(
            self, farm: Farm, generated_date: date, forecast_days: int = 13,
            forecast_fields: list = None
    ):
        """Initialize the report model for the Insight Request Report."""
        self.generated_date = generated_date
        self.farm = farm

        # Update data
        self.farm_id = self.farm.unique_id
        self.phone_number = self.farm.phone_number

        geometry = farm.geometry
        self.latitude = round(geometry.y, 4) if geometry else ''
        self.longitude = round(geometry.x, 4) if geometry else ''

        # Forecast
        forecast = FarmShortTermForecast.objects.filter(
            farm=self.farm,
            forecast_date=self.generated_date
        ).first()

        # Check if forecast is found
        if not forecast:
            self.forecast = FarmShortTermForecastData.objects.none()
        else:
            self.forecast = forecast.farmshorttermforecastdata_set.order_by(
                'value_date'
            )

        # Make default forecast_fields
        if not forecast_fields:
            forecast_fields = CropPlanData.default_fields()

        self.forecast = self.forecast.filter(
            dataset_attribute__source__in=forecast_fields
        ).order_by('value_date')

        self.forecast_fields = forecast_fields
        self.forecast_days = forecast_days

    @property
    def data(self) -> dict:
        """Return the data."""
        # ---------------------------------------
        # Spw data
        spw_top_message = ''
        spw_description = ''
        spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm,
            generated_date=self.generated_date

        ).first()
        if spw:
            try:
                spw_output = SPWOutput.objects.get(identifier=spw.signal)
                spw_top_message = spw_output.plant_now_string
                spw_description = spw_output.description
            except SPWOutput.DoesNotExist:
                spw_top_message = spw.signal
                spw_description = ''
        # ---------------------------------------

        output = {
            'farmID': self.farm.unique_id,
            'phoneNumber': self.farm.phone_number,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'SPWTopMessage': spw_top_message,
            'SPWDescription': spw_description,
        }

        # ----------------------------------------
        # Short term forecast data
        for idx in range(self.forecast_days):
            for field in self.forecast_fields:
                output[f'day{idx + 1}_{field}'] = ''

        first_date = None
        if self.forecast.first():
            first_date = self.forecast.first().value_date

        if first_date:
            # Create forecast data
            for data in self.forecast:
                var = data.dataset_attribute.source
                day_n = (data.value_date - first_date).days + 1
                output[f'day{day_n}_{var}'] = data.value

                if (var == 'rainAccumulationSum' and
                        'rainAccumulationType' in self.forecast_fields):
                    # we get the rain type
                    _class = RainfallClassification.classify(data.value)
                    if _class:
                        output[
                            f'day{day_n}_rainAccumulationType'
                        ] = _class.name
        return output


class CropInsightRequest(models.Model):
    """Crop insight request."""

    unique_id = models.UUIDField(
        default=uuid.uuid4, editable=False
    )
    requested_by = models.ForeignKey(
        User, on_delete=models.CASCADE
    )
    requested_date = models.DateField(
        default=timezone.now,
        help_text='Date when the request is made'
    )
    farms = models.ManyToManyField(Farm)
    file = models.FileField(
        upload_to=ingestor_file_path,
        null=True, blank=True
    )

    def generate_report(self):
        """Generate reports."""
        output = []

        # If farm is empty, put empty farm
        farms = self.farms.all()
        if not farms.count():
            farms = [Farm()]

        # Get farms
        for farm in farms:
            data = CropPlanData(
                farm, self.requested_date,
                forecast_fields=[
                    'rainAccumulationSum', 'precipitationProbability',
                    'rainAccumulationType'
                ]
            ).data

            # Create header
            if len(output) == 0:
                output.append(list(data.keys()))
            output.append([val for key, val in data.items()])

        # Render csv
        csv_content = ''

        # Replace header
        output[0] = json.loads(
            json.dumps(output[0]).
            replace('rainAccumulationSum', 'mm').
            replace('rainAccumulationType', 'Type').
            replace('precipitationProbability', 'Chance')
        )
        for row in output:
            csv_content += ','.join(map(str, row)) + '\n'
        content_file = ContentFile(csv_content)
        self.file.save(f'{self.unique_id}.csv', content_file)
        self.save()

        # Send email
        email = EmailMessage(
            subject=(
                "GAP - Crop Plan Generator Results - "
                f"{self.generated_date.strftime('%A-%d-%m-%Y')}"
            ),
            body='''
Hi everyone,


Please find the attached file for the crop plan generator results.


Best regards
            ''',
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[
                email for email in
                crop_plan_receiver().values_list('email', flat=True)
                if email
            ]
        )
        email.attach(
            f'{self.unique_id}.csv',
            self.file.open('rb').read(),
            'text/csv'
        )
        email.send()
