from enum import Enum

class ScannerThreadName(str, Enum):
    SMALL_CAP_INITIAL_POP_SCANNER = 'SmallCapInitialPopScanner'
    YESTERDAY_TOP_GAINER = 'YesterdayTopGainer'