from enum import Enum

class CustomisedIndicator(str, Enum):
    PREVIOUS_CLOSE = 'Previous Close'
    CLOSE_CHANGE = 'Close Change'
    PREVIOUS_CLOSE_CHANGE = 'Previous Close Change'
    CANDLE_COLOUR = 'Candle Colour'
    CANDLE_UPPER_BODY = 'Candle Upper Body'
    CANDLE_LOWER_BODY = 'Candle Lower Body'
    MARUBOZU_RATIO = 'Marubozu Ratio'
    BODY_DIFF_RATIO = 'Body Diff Ratio'
    VWAP = 'Vwap'
    TOTAL_VOLUME = 'Total Volume'
    MA_20_VOLUME = '20MA Volume'
    MA_50_VOLUME = '50MA Volume'