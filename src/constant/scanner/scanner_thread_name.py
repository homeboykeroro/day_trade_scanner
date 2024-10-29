from enum import Enum

class ScannerThreadName(str, Enum):
    SMALL_CAP_INITIAL_POP_SCANNER = 'SmallCapInitialPopScanner'
    YESTERDAY_TOP_GAINER = 'YesterdayTopGainer'
    PREVIOUS_DAY_TOP_GAINER_CONTINUATION = 'PreviousDayTopGainerContinuation'
    PREVIOUS_DAY_TOP_GAINER_SUPPORT = 'PreviousDayTopGainerSupport'
    INTRA_DAY_BREAKOUT = 'IntraDayBreakout'
    