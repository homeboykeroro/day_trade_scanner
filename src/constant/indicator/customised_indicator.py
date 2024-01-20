from enum import Enum

class CustomisedIndicator(str, Enum):
    CLOSE_CHANGE = 'Close Change'
    GAP_PCT_CHANGE = 'Gap Percent Change'
    CANDLE_COLOUR = 'Candle Colour'
    TOTAL_VOLUME = 'Total Volume'
    CANDLE_UPPER_BODY = 'Candle Upper Body'
    CANDLE_LOWER_BODY = 'Candle Lower Body'