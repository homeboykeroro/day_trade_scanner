from enum import Enum

class ScannerTarget(str, Enum):
    TOP_GAINER = 'TOP_GAINER',
    TOP_LOSER = 'TOP_LOSER',
    YESTERDAY_TOP_GAINER = 'YESTERDAY_TOP_GAINER'