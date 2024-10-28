# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Variable that being used for PRISE.
"""


class PriseMessageGroup:
    """Type of message that being used by Prise."""

    START_SEASON = 'Start of Season'
    TIME_TO_ACTION_1 = 'PRISE Time To Action 1 - Beginning of the month'
    TIME_TO_ACTION_2 = 'PRISE Time To Action 2 - Second half of the month'
    END_SEASON = 'End of season'

    @classmethod
    def groups(cls):
        """Return all available groups."""
        return [
            cls.START_SEASON, cls.TIME_TO_ACTION_1, cls.TIME_TO_ACTION_2,
            cls.END_SEASON
        ]


class PriseDataType:
    """Type of data that being used by Prise."""

    CLIMATOLOGY = 'Climatology'
    NEAR_REAL_TIME = 'Near Real Time'

    @classmethod
    def types(cls):
        """Return all available types."""
        return [cls.CLIMATOLOGY, cls.NEAR_REAL_TIME]

    @classmethod
    def get_type_by_message_group(cls, group: str):
        """Return type by message group (TTA1/TTA2)."""
        if group == PriseMessageGroup.TIME_TO_ACTION_1:
            return cls.CLIMATOLOGY
        elif group == PriseMessageGroup.TIME_TO_ACTION_2:
            return cls.NEAR_REAL_TIME
        return None


class PriseMessageContextVariable:
    """Available context that being used in Prise Messages."""

    FARM_GROUP_PHONE = 'phone'
    PLANTING_PREVIOUS_MONTH = 'last_month'
    PLANTING_CURRENT_MONTH = 'month'
    PEST_WINDOW_DAY_1 = 'day_1'
    PEST_WINDOW_DAY_2 = 'day_2'
