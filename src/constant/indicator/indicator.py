from enum import Enum

class Indicator(str, Enum):
    OPEN = 'Open'
    HIGH = 'High'
    LOW = 'Low'
    CLOSE = 'Close'
    VOLUME = 'Volume'
    HIGHEST = 'Highest'
    LOWEST = 'Lowest'
    VOLUME_ON_HIGHEST = 'Volume on Highest'
    VOLUME_ON_LOWEST = 'Volume on Lowest'
    TIME_AT_HIGHEST = 'Time at Highest'
    TIME_AT_LOWEST = 'Time at Lowest'