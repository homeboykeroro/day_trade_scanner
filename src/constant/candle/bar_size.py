from enum import Enum

class BarSize(str, Enum):
    ONE_MINUTE = '1min'
    FIVE_MINUTE = '5min'
    ONE_DAY = '1d'