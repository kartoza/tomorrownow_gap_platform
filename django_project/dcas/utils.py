# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Utilities
"""

import pandas as pd
import duckdb


def read_grid_data(
    parquet_file_path, column_list: list, grid_id_list: list
) -> pd.DataFrame:
    """Read grid data from parquet file.

    :param parquet_file_path: file_path to parquet file
    :type parquet_file_path: str
    :param column_list: List of column to be read
    :type column_list: list
    :param grid_id_list: List of grid_id to be filtered
    :type grid_id_list: list
    :return: DataFrame that contains grid_id and column_list
    :rtype: pd.DataFrame
    """
    conndb = duckdb.connect()
    query = (
        f"""
        SELECT {','.join(column_list)}
        FROM read_parquet('{parquet_file_path}')
        WHERE grid_id IN {list(grid_id_list)}
        """
    )
    df = conndb.sql(query).df()
    conndb.close()
    return df


def read_grid_crop_data(
    parquet_file_path, grid_id_list: list,
    crop_id_list: list, crop_stage_type_list: list
) -> pd.DataFrame:
    """Read grid data from parquet file.

    :param parquet_file_path: file_path to parquet file
    :type parquet_file_path: str
    :param grid_id_list: List of grid_id to be filtered
    :type grid_id_list: list
    :param crop_id_list: List of crop_id to be filtered
    :type crop_id_list: list
    :param crop_stage_type_list: List of crop_stage_type to be filtered
    :type crop_stage_type_list: list
    :return: DataFrame that contains grid_id and column_list
    :rtype: pd.DataFrame
    """
    conndb = duckdb.connect()
    query = (
        f"""
        SELECT *
        FROM read_parquet('{parquet_file_path}')
        WHERE grid_id IN {list(grid_id_list)} AND
        crop_id IN {list(crop_id_list)} AND
        crop_stage_type_id IN {list(crop_stage_type_list)}
        """
    )
    df = conndb.sql(query).df()
    conndb.close()
    return df


def print_df_memory_usage(df: pd.DataFrame):
    """Print dataframe memory usage.

    :param df: dataframe
    :type df: pd.DataFrame
    """
    memory = df.memory_usage(deep=True)
    total_memory = memory.sum()  # Total memory usage in bytes

    print(f"Total memory usage: {total_memory / 1024:.2f} KB")
    # # 760 MB for 33K grid data
