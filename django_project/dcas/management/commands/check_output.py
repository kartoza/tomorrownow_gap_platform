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

    def export_to_csv(self, sql):
        conn = duckdb.connect()
        final_query = (
            f"""
            COPY({sql})
            TO 'output.csv'
            (HEADER, DELIMITER ',');
            """
        )
        conn.sql(final_query)
        conn.close()

    def handle(self, *args, **options):
        """Run DCAS Pipeline."""
        grid_path = os.path.join(
            '/tmp', 'dcas', 'grid_crop'
        )
        
        sql = (
            f"""
            SELECT *
            FROM read_parquet('{grid_path}/*.parquet')
            """
        )
        self.export_to_csv(sql)

        # conn = duckdb.connect()
        # conn.sql(sql).show()
        # conn.close()
