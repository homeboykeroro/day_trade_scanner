from enum import Enum

class ScannerThreadName(str, Enum):
    SMALL_CAP_INITIAL_POP_SCANNER = 'SmallCapInitialPopScanner'
    YESTERDAY_TOP_GAINER_SCANNER = 'YesterdayTopGainerScanner'
    PREVIOUS_DAY_TOP_GAINER_CONTINUATION = 'PreviousDayTopGainerContinuationScanner'
    PREVIOUS_DAY_TOP_GAINER_SUPPORT = 'PreviousDayTopGainerSupportScanner'
    INTRA_DAY_BREAKOUT_SCANNER = 'IntraDayBreakoutScanner'
    IPO_INFO_SCRAPER = 'IPOInfoScraper'