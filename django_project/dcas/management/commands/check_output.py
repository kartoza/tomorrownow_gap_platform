# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import logging
import os
from django.core.management.base import BaseCommand

import duckdb


logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    """Command to process DCAS Pipeline."""

    def handle(self, *args, **options):
        """Run DCAS Pipeline."""
        grid_path = os.path.join(
            '/tmp', 'dcas', 'grid_crop'
        )
        conn = duckdb.connect()
        sql = (
            f"""
            SELECT grid_id, crop_id, crop_stage_type_id,
            planting_date, growth_stage_id,
            message, message_2, message_3, message_4, message_5
            FROM read_parquet('{grid_path}/*.parquet');
            """
        )
        conn.sql(sql).show()
        conn.close()
