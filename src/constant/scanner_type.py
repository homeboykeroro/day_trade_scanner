from enum import Enum

class ScannerType(str, Enum):
    DAY_TRADE = 'DAY_TRADE',
    SWING_TRADE = 'SWING_TRADE'