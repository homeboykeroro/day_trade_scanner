from enum import Enum

class ScannerThreadName(str, Enum):
    SMALL_CAP_INITIAL_POP_SCANNER = 'SmallCapInitialPopScanner'
    SMALL_CAP_INTRA_DAY_BREAKOUT_SCANNER = 'SmallCapIntraDayBreakoutScanner'
    YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_SCANNER = 'YesterdayTopGainerBullishDailyCandleScanner'
    IPO_INFO_SCRAPER = 'IpoInfoScraper'
    PREVIOUS_DAY_TOP_GAINER_CONTINUATION = 'PreviousDayTopGainerContinuationScanner'
    PREVIOUS_DAY_TOP_GAINER_SUPPORT = 'PreviousDayTopGainerSupportScanner'