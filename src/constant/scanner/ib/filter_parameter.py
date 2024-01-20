from enum import Enum

class FilterParameter(str, Enum):
    MIN_PRICE = 'priceAbove'
    MIN_PERCENT_CHANGE = 'changePercAbove'
    MAX_PERCENT_CHANGE = 'changePercBelow'
    MIN_PERCENT_CHANGE_AFTER_HOURS = 'afterHoursChangePercAbove'
    MAX_PERCENT_CHANGE_AFTER_HOURS = 'afterHoursChangePercBelow'
    MIN_USD_VOLUME = 'usdVolumeAbove'
    MAX_MARKET_CAP = 'marketCapBelow1e6'