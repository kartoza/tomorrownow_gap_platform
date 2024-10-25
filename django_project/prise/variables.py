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

    NEAR_REAL_TIME = 'Near Real Time'

    @classmethod
    def types(cls):
        """Return all available types."""
        return [cls.NEAR_REAL_TIME]
