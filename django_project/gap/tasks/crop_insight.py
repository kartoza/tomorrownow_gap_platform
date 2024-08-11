# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Crop insight Tasks.
"""

from celery.utils.log import get_task_logger
from django.contrib.auth import get_user_model

from core.celery import app
from gap.models.crop_insight import CropInsightRequest
from gap.models.farm import Farm
from spw.generator.crop_insight import CropInsightFarmGenerator

logger = get_task_logger(__name__)
User = get_user_model()


@app.task
def generate_spw(farms_id: list):
    """Generate spw."""
    for farm in Farm.objects.filter(id__in=farms_id):
        CropInsightFarmGenerator(farm).generate_spw()


@app.task
def generate_insight_report(_id: list):
    """Generate insight report."""
    request = CropInsightRequest.objects.get(id=_id)
    request.generate_report()


@app.task(name="generate_crop_plan")
def generate_crop_plan():
    """Generate crop plan for registered farms."""
    farms = Farm.objects.all().order_by('id')
    # generate crop insight for all farms
    for farm in farms:
        CropInsightFarmGenerator(farm).generate_spw()
    # create report request
    request = CropInsightRequest.objects.create(
        requested_by=User.objects.filter(
            is_superuser=True
        ).first()
    )
    request.farms.set(farms)
    # generate report
    request.generate_report()
