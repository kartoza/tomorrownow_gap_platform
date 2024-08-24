"""Tomorrow Now GAP."""
from __future__ import absolute_import, unicode_literals

import os
import logging

from celery import Celery
from celery.result import AsyncResult
from celery.schedules import crontab


logger = logging.getLogger(__name__)

# set the default Django settings module for the 'celery' program.
# this is also used in manage.py
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

# Get the base REDIS URL, default to redis' default
BASE_REDIS_URL = (
    f'redis://default:{os.environ.get("REDIS_PASSWORD", "")}'
    f'@{os.environ.get("REDIS_HOST", "")}',
)

app = Celery('GAP')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

app.conf.broker_url = BASE_REDIS_URL

# this allows you to schedule items in the Django admin.
app.conf.beat_scheduler = 'django_celery_beat.schedulers.DatabaseScheduler'

# Task cron job schedules
app.conf.beat_schedule = {
    'netcdf-s3-sync': {
        'task': 'netcdf_s3_sync',
        'schedule': crontab(minute='0', hour='1'),  # Run everyday at 1am UTC
    },
    'generate-crop-plan': {
        'task': 'generate_crop_plan',
        # Run everyday at 2am East Africa Time or 23:00 UTC
        'schedule': crontab(minute='30', hour='1'),
    },
}


def cancel_task(task_id: str):
    """
    Cancel task if it's ongoing.

    :param task_id: task identifier
    """
    try:
        res = AsyncResult(task_id)
        if not res.ready():
            # find if there is running task and stop it
            app.control.revoke(
                task_id,
                terminate=True,
                signal='SIGKILL'
            )
    except Exception as ex:
        logger.error(f'Failed cancel_task: {task_id}')
        logger.error(ex)
