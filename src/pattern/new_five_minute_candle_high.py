import time
import pandas as pd
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.dataframe_util import derive_idx_df
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class NewFiveMinuteCandleHigh(PatternAnalyser):
    MIN_GAP_UP_PCT = 4
    MIN_PREVIOUS_CLOSE_PCT = 15
    OBSERVE_PERIOD = 5
    
    def __init__(self, historical_data_df: DataFrame, ticker_to_contract_dict: dict):
        self.__historical_data_df = historical_data_df
        self.__ticker_to_contract_dict = ticker_to_contract_dict
        
    def analyse(self) -> None:
        logger.log_debug_msg('New five minute candle high scan')
        start_time = time.time()
        
        previous_close_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE.value]]
        previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE.value]]
        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
        candle_lower_body_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_BODY.value]]
        
        gap_up_pct_df = (((candle_lower_body_df.sub(previous_close_df.value))
                                               .div(candle_lower_body_df.values))
                                               .mul(100))
        
        min_gap_up_pct_boolean_df = (gap_up_pct_df >= self.MIN_GAP_UP_PCT).rename(columns={CustomisedIndicator.CANDLE_LOWER_BODY.value: RuntimeIndicator.COMPARE.value})
        min_previous_close_pct_boolean_df = (previous_close_pct_df >= self.MIN_PREVIOUS_CLOSE_PCT).rename(columns={CustomisedIndicator.PREVIOUS_CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
        is_not_grey_boolean_df = (candle_colour_df != CandleColour.GREY.value).rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        pop_up_boolean_df = (min_gap_up_pct_boolean_df) & (min_previous_close_pct_boolean_df) & (is_not_grey_boolean_df)

        numeric_idx_df = derive_idx_df(pop_up_boolean_df)
        first_occurrence_start_idx_np = numeric_idx_df.where(pop_up_boolean_df.values).idxmin().values
        first_occurrence_end_idx_np = first_occurrence_start_idx_np + self.OBSERVE_PERIOD
        
        first_pop_up_occurrence_df = (numeric_idx_df >= first_occurrence_start_idx_np) & (numeric_idx_df <= first_occurrence_end_idx_np)
        
        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
        high_df = self.__historical_data_df.loc[:, idx[:, Indicator.HIGH.value]]
        pop_up_five_minute_candle_df = high_df.where(first_pop_up_occurrence_df.values)
        pop_up_five_minute_candle_df.max()
        
