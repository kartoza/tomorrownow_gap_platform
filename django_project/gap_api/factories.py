# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for Models
"""
import factory
from factory.django import DjangoModelFactory

from core.factories import UserF
from gap_api.models import APIRequestLog


class APIRequestLogFactory(DjangoModelFactory):
    """Factory class for APIRequestLog model."""

    class Meta:  # noqa
        model = APIRequestLog

    user = factory.SubFactory(UserF)
    username_persistent = factory.Faker('name')
    path = '/api/v1/measurement'
    host = 'http://localhost'
    method = 'GET'
    query_params = {
        'lat': '-1.404244',
        'lon': '35.008688',
        'product': 'tahmo_ground_observation',
        'end_date': '2019-11-10',
        'attributes': [
            'max_relative_humidity',
            'min_relative_humidity'
        ],
        'start_date': '2019-11-01',
        'output_type': 'csv'
    }
