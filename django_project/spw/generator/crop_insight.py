# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Farm SPW Generator.
"""

from datetime import date, datetime, timedelta

import pytz

from gap.models.crop_insight import (
    FarmSuitablePlantingWindowSignal, FarmShortTermForecast,
    FarmShortTermForecastData
)
from gap.models.farm import Farm
from gap.models.measurement import DatasetAttribute
from spw.generator.main import (
    calculate_from_point, VAR_MAPPING_REVERSE, _fetch_timelines_data_dataset
)


class CropInsightFarmGenerator:
    """Insight Farm Generator."""

    def __init__(self, farm: Farm):
        """Init Generator."""
        self.farm = farm
        self.today = date.today()
        self.tomorrow = self.today + timedelta(days=1)

    def generate_spw(self):
        """Generate Farm SPW."""
        # Check already being generated, no regenereated!
        if FarmSuitablePlantingWindowSignal.objects.filter(
                farm=self.farm,
                generated_date=self.today
        ).first():
            return

        output, historical_dict = calculate_from_point(
            self.farm.geometry
        )
        dataset = _fetch_timelines_data_dataset()

        # Save SPW
        FarmSuitablePlantingWindowSignal.objects.update_or_create(
            farm=self.farm,
            generated_date=self.today,
            defaults={
                'signal': output.data.goNoGo
            }
        )

        # Save the short term forecast
        for k, v in historical_dict.items():
            _date = datetime.strptime(v['date'], "%Y-%m-%d")
            _date = _date.replace(tzinfo=pytz.UTC)
            if self.tomorrow <= _date.date():
                for attr_name, val in v.items():
                    try:
                        attr = DatasetAttribute.objects.filter(
                            dataset=dataset,
                            attribute__variable_name=VAR_MAPPING_REVERSE[
                                attr_name
                            ]
                        ).first()
                        if attr:
                            c, _ = FarmShortTermForecast.objects.get_or_create(
                                farm=self.farm,
                                forecast_date=self.today
                            )
                            FarmShortTermForecastData.objects.update_or_create(
                                forecast=c,
                                value_date=_date,
                                dataset_attribute=attr,
                                defaults={
                                    'value': val
                                }
                            )
                    except KeyError:
                        pass
