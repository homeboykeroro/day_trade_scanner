import time

from pandas.core.frame import DataFrame
import numpy as np
import pandas as pd

from pattern.pattern_analyser import PatternAnalyser

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.logger import Logger
from utils.dataframe_util import derive_idx_df, get_sorted_value_without_duplicate_df, get_idx_df_by_value_df

idx = pd.IndexSlice
logger = Logger()

class SupportAndResistence(PatternAnalyser):
    MIN_CLOSE_CHANGE = 8
    MIN_TIME_INTERVAL = 6
    
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        logger.log_debug_msg('Consolidation scan')
        start_time = time.time()
        
        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
        close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        previous_close_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE]]
        previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]]
        high_df = self.__historical_data_df.loc[:, idx[:, Indicator.HIGH]].rename(columns={Indicator.HIGH: RuntimeIndicator.COMPARE})
        volume_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]].rename(columns={Indicator.VOLUME: RuntimeIndicator.COMPARE})
        total_volume_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.TOTAL_VOLUME]]
        
        timeframe_length = len(close_df)
        
        get_sorted_df_start_time = time.time()
        sorted_close_df = get_sorted_value_without_duplicate_df(close_df)
        sorted_close_idx_df = get_idx_df_by_value_df(sorted_close_df, close_df)
        sorted_close_datetime_idx_df = get_idx_df_by_value_df(sorted_close_df, close_df, numeric_idx = False)
        
        
        