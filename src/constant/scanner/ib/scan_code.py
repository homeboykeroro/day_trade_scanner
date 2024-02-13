from enum import Enum

class ScanCode(str, Enum):
    TOP_GAINERS = 'TOP_PERC_GAIN'
    TOP_GAINER_SINCE_OPEN = 'TOP_OPEN_PERC_GAIN'
    TOP_GAINERS_IN_AFTER_HOURS = 'TOP_AFTER_HOURS_PERC_GAIN'
    TOP_LOSERS = 'TOP_PERC_LOSE'
    TOP_LOSER_SINCE_OPEN = 'TOP_OPEN_PERC_LOSE'
    TOP_LOSER_IN_AFTER_HOURS = 'TOP_AFTER_HOURS_PERC_LOSE'
    CLOSEST_TO_HALT = 'LIMIT_UP_DOWN'
    HALTED = 'HALTED'