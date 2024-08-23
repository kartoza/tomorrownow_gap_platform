# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Models
"""

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

    def __init__(self, farm: Farm, requested_date: date):
        """Initialize the report model for the Insight Request Report."""
        self.forecast = {}
        self.requested_date = requested_date
        self.farm = farm

        # Update data
        self.farm_id = self.farm.unique_id
        self.phone_number = self.farm.phone_number
        self.latitude = farm.geometry.y
        self.longitude = farm.geometry.x
        self.spw_top_message = ''
        self.spw_description = ''

        self.update_data()

    def update_data(self):
        """Update data for the attributes."""
        # Get spw data
        spw = FarmSuitablePlantingWindowSignal.objects.filter(
            farm=self.farm,
            generated_date=self.requested_date

        ).first()
        if spw:
            try:
                spw_output = SPWOutput.objects.get(identifier=spw.signal)
                self.spw_top_message = spw_output.plant_now_string
                self.spw_description = spw_output.description
            except SPWOutput.DoesNotExist:
                self.spw_top_message = spw.signal
                self.spw_description = ''

        # Forecast
        forecast = FarmShortTermForecast.objects.filter(
            farm=self.farm,
            forecast_date=self.requested_date
        ).first()

        # Skip if no forecast
        if not forecast:
            return
        datas = forecast.farmshorttermforecastdata_set.order_by(
            'value_date'
        )

        # Skip if no data
        if not datas.first():
            return

        first_date = datas.first().value_date

        # Create data
        for data in datas:
            var = data.dataset_attribute.source

            day_n = (data.value_date - first_date).days + 1
            if day_n not in self.forecast:
                self.forecast[day_n] = {}

            self.forecast[day_n][var] = data.value
            if var == 'rainAccumulationSum':
                self.forecast[day_n]['rainAccumulationType'] = ''

                # we get the rain type
                _class = RainfallClassification.classify(
                    data.value
                )
                if _class:
                    self.forecast[day_n]['rainAccumulationType'] = _class.name


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
        output = [
            [
                'farmID', 'phoneNumber',
                'latitude', 'longitude', 'SPWTopMessage', 'SPWDescription'
            ],
        ]
        last_max_day = 0
        for farm in self.farms.all():
            data = CropPlanData(farm, self.requested_date)
            row = [
                data.farm_id,
                data.phone_number,
                round(data.latitude, 4),
                round(data.longitude, 4),
                data.spw_top_message,
                data.spw_description
            ]

            # We add the forecast
            forecast_keys = data.forecast.keys()
            max_day = last_max_day
            if forecast_keys:
                max_day = max([int(key) for key in data.forecast.keys()])
                last_max_day = max_day

            # Save the data
            for idx in range(last_max_day):
                # Create the header
                day_n = idx + 1
                header_1 = f'day{day_n}_mm'
                header_2 = f'day{day_n}_Chance'
                header_3 = f'day{day_n}_Type'
                if header_1 not in output[0]:
                    output[0] += [
                        header_1, header_2, header_3
                    ]
                # Get forecast data
                try:
                    forecast_data = data.forecast[day_n]
                except KeyError:
                    forecast_data = {}
                try:
                    value_1 = forecast_data['rainAccumulationSum']
                except KeyError:
                    value_1 = ''
                try:
                    value_2 = forecast_data['precipitationProbability']
                except KeyError:
                    value_2 = ''
                try:
                    value_3 = forecast_data['rainAccumulationType']
                except KeyError:
                    value_3 = ''
                row += [value_1, value_2, value_3]
            output.append(row)

        # Render csv
        csv_content = ''
        for row in output:
            csv_content += ','.join(map(str, row)) + '\n'
        content_file = ContentFile(csv_content)
        self.file.save(f'{self.unique_id}.csv', content_file)
        self.save()

        # Send email
        email = EmailMessage(
            subject=(
                "GAP - Crop Plan Generator Results - "
                f"{self.requested_date.strftime('%A-%d-%m-%Y')}"
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
