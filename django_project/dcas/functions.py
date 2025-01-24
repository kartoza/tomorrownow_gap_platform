# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: DCAS Functions to process row data.
"""

import pandas as pd

from dcas.rules.rule_engine import DCASRuleEngine
from dcas.rules.variables import DCASData
from dcas.service import GrowthStageService


def calculate_growth_stage(
    row: pd.Series, epoch_list: list
) -> pd.Series:
    """Identify the growth stage and its start date.

    The calculation will be using GDD cumulative sum for each day.
    :param row: single row
    :type row: pd.Series
    :param epoch_list: list of processing date epoch
    :type epoch_list: list
    :return: row with growth_stage_id and growth_stage_start_date
    :rtype: pd.Series
    """
    # possible scenario:
    # - no prev_growth_stage_start_date or prev_growth_stage_id
    # - growth_stage_id is the same with prev_growth_stage_id
    # - growth_stage_id is different with prev_growth_stage_id
    #
    # for 2nd scenario, use the prev_growth_stage_start_date
    # for 1st and 3rd scenario, we need to find the date that
    # growth stage is changed

    # check cumulative GDD from last value
    growth_stage_dict = GrowthStageService.get_growth_stage(
        row['crop_id'],
        row['crop_stage_type_id'],
        row[f'gdd_sum_{epoch_list[-1]}'],
        row['config_id']
    )

    if growth_stage_dict is None:
        # no lookup value
        row['growth_stage_id'] = row['prev_growth_stage_id']
        row['growth_stage_start_date'] = row['prev_growth_stage_start_date']
        return row
    print(growth_stage_dict)
    gdd_threshold = growth_stage_dict['gdd_threshold']
    row['growth_stage_id'] = growth_stage_dict['id']

    row['growth_stage_start_date'] = row['prev_growth_stage_start_date']
    if (
        not pd.isna(row['prev_growth_stage_id']) and
        row['growth_stage_id'] == row['prev_growth_stage_id']
    ):
        # the growth_stage_id is not changed
        return row

    if gdd_threshold == 0:
        # if threshold is 0, then we return the plantingDate
        row['growth_stage_start_date'] = row['planting_date_epoch']
        return row

    found = False
    prev_epoch = epoch_list[-1]
    for idx, epoch in reversed(list(enumerate(epoch_list))):
        if idx == len(epoch_list) - 1:
            # skip last item
            continue

        if epoch < row['planting_date_epoch']:
            row['growth_stage_start_date'] = row['planting_date_epoch']
            found = True
            break

        sum_gdd = row[f'gdd_sum_{epoch}']

        if sum_gdd <= gdd_threshold:
            # found first sum_gdd that is
            # lesser than or equal to the lower threshold
            row['growth_stage_start_date'] = prev_epoch
            found = True
            break
        prev_epoch = epoch

    if not found:
        # if not found, then we assign the first epoch
        row['growth_stage_start_date'] = epoch_list[0]

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
        row['config_id'],
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
