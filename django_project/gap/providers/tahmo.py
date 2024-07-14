# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Data Reader
"""

from typing import List
from datetime import datetime
from django.contrib.gis.geos import Point
from django.contrib.gis.db.models.functions import Distance

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement
)
from gap.utils.reader import (
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseDatasetReader
)


class TahmoDatasetReader(BaseDatasetReader):
    """Class to read Tahmo ground observation data."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            point: Point, start_date: datetime, end_date: datetime) -> None:
        """Initialize TahmoDatasetReader class.

        :param dataset: Dataset from Tahmo provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param point: Location to be queried
        :type point: Point
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(dataset, attributes, point, start_date, end_date)
        self.results = []

    def read_historical_data(self):
        """Read historical data from dataset."""
        nearest_station = Station.objects.annotate(
            distance=Distance('geometry', self.point)
        ).filter(
            provider=self.dataset.provider
        ).order_by('distance').first()
        if nearest_station is None:
            return
        measurements = Measurement.objects.select_related(
            'dataset_attribute', 'dataset_attribute__attribute'
        ).filter(
            date_time__gte=self.start_date,
            date_time__lte=self.end_date,
            dataset_attribute__in=self.attributes,
            station=nearest_station
        ).order_by('date_time')
        curr_dt = None
        measurement_dict = {}
        for measurement in measurements:
            if curr_dt is None:
                curr_dt = measurement.date_time
            elif curr_dt != measurement.date_time:
                self.results.append(
                    DatasetTimelineValue(curr_dt, measurement_dict))
                curr_dt = measurement.date_time
                measurement_dict = {}
            measurement_dict[
                measurement.dataset_attribute.attribute.variable_name
            ] = measurement.value
        self.results.append(
            DatasetTimelineValue(curr_dt, measurement_dict))

    def read_forecast_data(self):
        """Read forecast data from dataset."""
        raise NotImplementedError(
            'Tahmo does not have forecast data implementation!')

    def get_data_values(self) -> DatasetReaderValue:
        """Fetch results.

        :return: Data Value.
        :rtype: DatasetReaderValue
        """
        metadata = {
            'dataset': [self.dataset.name],
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat()
        }
        return DatasetReaderValue(metadata, self.results)
