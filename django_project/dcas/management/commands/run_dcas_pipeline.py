# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Run DCAS Data Pipeline
"""

import logging
import os
import random
import uuid
import datetime
import fsspec
import numpy as np
import pandas as pd
from pprint import pprint
from django.db import connection
import dask.dataframe as dd
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, select, MetaData, event
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
import dask_geopandas as dg
from shapely import from_wkb
from dask_geopandas.io.parquet import to_parquet

from gap.models import (
    Grid, Farm, FarmRegistry, FarmRegistryGroup,
    Crop, CropStageType
)
from gap.utils.dask import execute_dask_compute
from dcas.models import DCASConfig
from dcas.pipeline import DCASDataPipeline


logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    """Command to process DCAS Pipeline."""

    def handle(self, *args, **options):
        """Run DCAS Pipeline."""
        dt = datetime.date(2024, 12, 1)
        config = DCASConfig.objects.get(id=1)
        farm_registry_group = FarmRegistryGroup.objects.get(id=1)

        pipeline = DCASDataPipeline(farm_registry_group, config, dt)

        pipeline.run()
