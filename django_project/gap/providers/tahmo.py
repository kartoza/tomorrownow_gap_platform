# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: CBAM Data Reader
"""

from typing import List
from datetime import datetime
from django.contrib.gis.db.models.functions import Distance

from gap.models import (
    Dataset,
    DatasetAttribute,
    Station,
    Measurement
)
from gap.utils.reader import (
    DatasetReaderInput,
    DatasetTimelineValue,
    DatasetReaderValue,
    BaseDatasetReader
)


class TahmoDatasetReader(BaseDatasetReader):
    """Class to read Tahmo ground observation data."""

    def __init__(
            self, dataset: Dataset, attributes: List[DatasetAttribute],
            location_input: DatasetReaderInput, start_date: datetime,
            end_date: datetime) -> None:
        """Initialize TahmoDatasetReader class.

        :param dataset: Dataset from Tahmo provider
        :type dataset: Dataset
        :param attributes: List of attributes to be queried
        :type attributes: List[DatasetAttribute]
        :param location_input: Location to be queried
        :type location_input: DatasetReaderInput
        :param start_date: Start date time filter
        :type start_date: datetime
        :param end_date: End date time filter
        :type end_date: datetime
        """
        super().__init__(
            dataset, attributes, location_input, start_date, end_date)
        self.results = []

    def read_historical_data(self, start_date: datetime, end_date: datetime):
        """Read historical data from dataset.

        :param start_date: start date for reading historical data
        :type start_date: datetime
        :param end_date:  end date for reading historical data
        :type end_date: datetime
        """
        nearest_station = Station.objects.annotate(
            distance=Distance('geometry', self.location_input.points[0])
        ).filter(
            provider=self.dataset.provider
        ).order_by('distance').first()
        if nearest_station is None:
            return
        measurements = Measurement.objects.select_related(
            'dataset_attribute', 'dataset_attribute__attribute'
        ).filter(
            date_time__gte=start_date,
            date_time__lte=end_date,
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
