# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Functions to process row data.
"""

import random
import pandas as pd

from dcas.rules.rule_engine import DCASRuleEngine
from dcas.rules.variables import DCASData


def calculate_growth_stage(
    row: pd.Series, growth_stage_list: list, current_date
) -> pd.Series:
    """Identify the growth stage and its start date.

    The calculation will be using GDD cumulative sum for each day.
    :param row: single row
    :type row: pd.Series
    :param growth_stage_list: list of growth stage
    :type growth_stage_list: list
    :param current_date: request date
    :type current_date: date
    :return: row with growth_stage_id and growth_stage_start_date
    :rtype: pd.Series
    """
    # TODO: lookup the growth_stage based on total_gdd value
    row['growth_stage_id'] = random.choice(growth_stage_list)

    # possible scenario:
    # - no prev_growth_stage_start_date or prev_growth_stage_id
    # - growth_stage_id is the same with prev_growth_stage_id
    # - growth_stage_id is different with prev_growth_stage_id
    #
    # for 2nd scenario, use the prev_growth_stage_start_date
    # for 1st and 3rd scenario, we need to find the date that
    # growth stage is changed

    if (
        row['prev_growth_stage_start_date'] is None or
        pd.isnull(row['prev_growth_stage_id']) or
        pd.isna(row['prev_growth_stage_id']) or
        row['growth_stage_id'] != row['prev_growth_stage_id']
    ):
        row['growth_stage_start_date'] = current_date
    return row


def calculate_message_output(
    row: pd.Series, rule_engine: DCASRuleEngine, attrib_dict: dict
) -> pd.Series:
    """Execute rule engine and get the message output.

    :param row: single row
    :type row: pd.Series
    :param rule_engine: Rule Engine
    :type rule_engine: DCASRuleEngine
    :param attrib_dict: attribute and its id
    :type attrib_dict: dict
    :return: row with message output columns
    :rtype: pd.Series
    """
    params = [
        {
            'id': attribute_id,
            'value': row[key]
        } for key, attribute_id in attrib_dict.items()
    ]
    data = DCASData(
        row['crop_id'],
        row['crop_stage_type_id'],
        row['growth_stage_id'],
        params
    )
    rule_engine.execute_rule(data)

    for idx, code in enumerate(data.message_codes):
        var_name = f'message_{idx + 1}' if idx > 0 else 'message'
        row[var_name] = code
    return row
