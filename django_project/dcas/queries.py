# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Functions to process row data.
"""

import pandas as pd
from sqlalchemy import create_engine, select, distinct, column, extract, func
from sqlalchemy.ext.automap import automap_base
from geoalchemy2.functions import ST_X, ST_Y, ST_Centroid


class DataQuery:
    """Class to build SQLQuery using sqlalchemy."""

    def __init__(self, connection_str, limit=None):
        """Initialize query builder."""
        self.connection_str = connection_str
        self.base_schema = None
        self.limit = limit

    @property
    def grid_id_index_col(self):
        """Get index column for Grid Data Query."""
        return 'gdid'

    @property
    def farmregistry_id_index_col(self):
        """Get index column for FarmRegistry Data Query."""
        return 'farmregistry_id'

    def setup(self):
        """Set the builder class."""
        self._init_schema()

    def _init_schema(self):
        # Create an SQLAlchemy engine
        engine = create_engine(self.connection_str)

        # Use automap base
        self.base_schema = automap_base()

        # Reflect the tables
        self.base_schema.prepare(engine, reflect=True)

        # Access reflected tables as classes
        # for table_name, mapped_class in self.base_schema.classes.items():
        #     if table_name != 'gap_farmregistry':
        #         continue
        #     print(f"Table: {table_name}, Class: {mapped_class}")
        #     pprint(vars(mapped_class.__table__))
        #     break

        # all accessed tables here
        self.farmregistry = (
            self.base_schema.classes['gap_farmregistry'].__table__
        )
        self.farm = self.base_schema.classes['gap_farm'].__table__
        self.cropstagetype = (
            self.base_schema.classes['gap_cropstagetype'].__table__
        )
        self.cropgrowthstage = (
            self.base_schema.classes['gap_cropgrowthstage'].__table__
        )
        self.crop = self.base_schema.classes['gap_crop'].__table__
        self.grid = self.base_schema.classes['gap_grid'].__table__
        self.country = self.base_schema.classes['gap_country'].__table__

    def grid_data_query(self, farm_registry_group):
        """Get query for Grid Data."""
        subquery = select(
            self.grid.c.id.label(self.grid_id_index_col),
            self.grid.c.id.label('grid_id'),
            self.country.c.iso_a3.label('iso_a3'),
            self.country.c.id.label('country_id'),
            ST_Centroid(self.grid.c.geometry).label('centroid')
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).join(
            self.country, self.grid.c.country_id == self.country.c.id
        ).where(
            self.farmregistry.c.group_id == farm_registry_group.id
        ).order_by(
            self.grid.c.id
        )

        if self.limit:
            # for testing purpose
            subquery = subquery.limit(self.limit)

        subquery = subquery.subquery('grid_data')
        return select(
            distinct(column(self.grid_id_index_col)),
            ST_Y(column('centroid')).label('lat'),
            ST_X(column('centroid')).label('lon'),
            column('grid_id'),
            column('iso_a3'),
            column('country_id'),
        ).select_from(subquery)

    def _grid_data_with_crop_subquery(self, farm_registry_group):
        return select(
            self.grid.c.id.label(self.grid_id_index_col),
            self.grid.c.id.label('grid_id'),
            self.farmregistry.c.crop_id,
            self.farmregistry.c.crop_stage_type_id,
            self.farmregistry.c.planting_date,
            extract(
                'epoch',
                func.DATE(self.farmregistry.c.planting_date)
            ).label('planting_date_epoch'),
            self.farmregistry.c.crop_growth_stage_id.label(
                'prev_growth_stage_id'
            ),
            extract(
                'epoch',
                func.DATE(self.farmregistry.c.growth_stage_start_date)
            ).label('prev_growth_stage_start_date'),
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).where(
            self.farmregistry.c.group_id == farm_registry_group.id
        ).order_by(
            self.grid.c.id
        )

    def grid_data_with_crop_query(self, farm_registry_group):
        """Get grid data with crop query."""
        subquery = self._grid_data_with_crop_subquery(farm_registry_group)
        if self.limit:
            # for testing purpose
            subquery = subquery.limit(self.limit)

        subquery = subquery.subquery('grid_data')
        return select(
            column(self.grid_id_index_col), column('crop_id'),
            column('crop_stage_type_id'), column('planting_date'),
            column('prev_growth_stage_id'),
            column('prev_growth_stage_start_date'),
            column('grid_id'),
            column('planting_date_epoch')
        ).distinct().select_from(subquery)

    def grid_data_with_crop_meta(self, farm_registry_group):
        """Get metadata for grid with crop data."""
        subquery = self._grid_data_with_crop_subquery(farm_registry_group)
        subquery = subquery.limit(1)
        subquery = subquery.subquery('grid_data')
        sql_query = select(
            column(self.grid_id_index_col),
            column('crop_id'),
            column('crop_stage_type_id'), column('planting_date'),
            column('prev_growth_stage_id'),
            column('prev_growth_stage_start_date'),
            column('grid_id'), column('planting_date_epoch')
        ).distinct().select_from(subquery)
        df = pd.read_sql_query(
            sql_query,
            con=self.connection_str,
            index_col=self.grid_id_index_col,
        )
        df['prev_growth_stage_id'] = (
            df['prev_growth_stage_id'].astype('Int64')
        )
        df['prev_growth_stage_start_date'] = (
            df['prev_growth_stage_start_date'].astype('Float64')
        )
        return df

    def _farm_registry_subquery(self, farm_registry_group):
        subquery = select(
            self.farmregistry.c.id.label('farmregistry_id'),
            self.farmregistry.c.planting_date.label('planting_date'),
            extract(
                'epoch',
                func.DATE(self.farmregistry.c.planting_date)
            ).label('planting_date_epoch'),
            self.farmregistry.c.crop_id.label('crop_id'),
            self.farmregistry.c.crop_growth_stage_id.label(
                'crop_growth_stage_id'
            ),
            self.farmregistry.c.crop_stage_type_id.label(
                'crop_stage_type_id'
            ),
            self.farmregistry.c.growth_stage_start_date.label(
                'growth_stage_start_date'
            ),
            self.farmregistry.c.group_id,
            self.farm.c.id.label('farm_id'),
            self.farm.c.unique_id.label('farm_unique_id'),
            self.farm.c.geometry.label('geometry'),
            self.grid.c.id.label('grid_id'),
            self.grid.c.unique_id.label('grid_unique_id'),
            self.farmregistry.c.id.label('registry_id'),
            self.cropgrowthstage.c.name.label('growth_stage'),
            (self.crop.c.name + '_' + self.cropstagetype.c.name).label('crop')
        ).select_from(self.farmregistry).join(
            self.farm, self.farmregistry.c.farm_id == self.farm.c.id
        ).join(
            self.grid, self.farm.c.grid_id == self.grid.c.id
        ).join(
            self.crop, self.farmregistry.c.crop_id == self.crop.c.id
        ).join(
            self.cropstagetype,
            self.farmregistry.c.crop_stage_type_id == self.cropstagetype.c.id
        ).join(
            self.cropgrowthstage,
            self.farmregistry.c.crop_growth_stage_id ==
            self.cropgrowthstage.c.id,
            isouter=True
        ).where(
            self.farmregistry.c.group_id == farm_registry_group.id
        ).order_by(
            self.grid.c.id, self.farmregistry.c.id
        )

        return subquery

    def farm_registry_query(self, farm_registry_group):
        """Get Farm Registry data query."""
        subquery = self._farm_registry_subquery(farm_registry_group)
        if self.limit:
            # for testing purpose
            subquery = subquery.limit(self.limit)

        subquery = subquery.subquery('farm_data')

        return select(subquery)

    def farm_registry_meta(self, farm_registry_group, request_date):
        """Get metadata for farm registry query."""
        subquery = self._farm_registry_subquery(farm_registry_group)
        subquery = subquery.limit(1)
        subquery = subquery.subquery('farm_data')

        sql_query = select(subquery)
        df = pd.read_sql_query(
            sql_query,
            con=self.connection_str,
            index_col=self.farmregistry_id_index_col,
        )

        df = df.assign(
            date=pd.Timestamp(request_date),
            year=lambda x: x.date.dt.year,
            month=lambda x: x.date.dt.month,
            day=lambda x: x.date.dt.day
        )
        return df
