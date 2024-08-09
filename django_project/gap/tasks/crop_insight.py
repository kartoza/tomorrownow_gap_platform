# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Crop insight Tasks.
"""

from celery.utils.log import get_task_logger

from core.celery import app
from gap.models.crop_insight import CropInsightRequest
from gap.models.farm import Farm
from spw.generator.crop_insight import CropInsightFarmGenerator

logger = get_task_logger(__name__)


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
