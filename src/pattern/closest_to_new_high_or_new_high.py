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

class ClosestToNewHighOrNewHigh(PatternAnalyser):
    NOTIFY_PERIOD = 2
    
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        logger.log_debug_msg('Closest to new high or new high scan')
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
        new_high_close_boolean_df = (sorted_close_idx_df.iloc[[-1]] >= timeframe_length - self.NOTIFY_PERIOD)
        new_high_close_result_series = new_high_close_boolean_df.any()
        new_high_close_ticker_list = new_high_close_result_series.index[new_high_close_result_series].get_level_values(0).tolist()
        
        sorted_high_df = get_sorted_value_without_duplicate_df(high_df)
        sorted_high_idx_df = get_idx_df_by_value_df(sorted_high_df, high_df)
        sorted_high_datetime_idx_df = get_idx_df_by_value_df(sorted_high_df, high_df, numeric_idx = False)
        new_high_boolean_df = (sorted_high_idx_df.iloc[[-1]] >= timeframe_length - self.NOTIFY_PERIOD)
        new_high_result_series = new_high_boolean_df.any()
        new_high_ticker_list = new_high_result_series.index[new_high_result_series].get_level_values(0).tolist()
        logger.log_debug_msg(f'get sorted df generation time: {time.time() - get_sorted_df_start_time} seconds')
       
        if len(new_high_close_ticker_list) > 0 or len(new_high_ticker_list) > 0:
            result_ticker_list = [new_high_close_ticker_list, new_high_ticker_list]

            for list_idx, ticker_list in enumerate(result_ticker_list):
                if len(ticker_list) > 0:
                    status = 'new high close' if (list_idx == 0) else 'new high'
                    datetime_idx_df = sorted_close_datetime_idx_df if (list_idx == 0) else sorted_high_datetime_idx_df
                    val_df = sorted_close_df if (list_idx == 0) else sorted_high_df

                    breakout_datetime = datetime_idx_df.loc[:, ticker].iat[0, 0]
                    ramp_up_hour = pd.to_datetime(breakout_datetime).hour
                    ramp_up_minute = pd.to_datetime(breakout_datetime).minute
                    display_hour = ('0' + str(ramp_up_hour)) if ramp_up_hour < 10 else ramp_up_hour
                    display_minute = ('0' + str(ramp_up_minute)) if ramp_up_minute < 10 else ramp_up_minute
                    display_time_str = f'{display_hour}:{display_minute}'
                    read_time_str = f'{ramp_up_hour} {ramp_up_minute}' if (ramp_up_minute > 0) else f'{ramp_up_hour} o clock' 
                    read_ticker_str = " ".join(ticker)
                    
                    display_close = val_df.loc[:, ticker].iat[0, 0]
                    display_close_pct = close_pct_df.loc[breakout_datetime, ticker]
                    display_previous_close = previous_close_df.loc[breakout_datetime, ticker]
                    display_previous_close_pct_change = previous_close_pct_df.loc[breakout_datetime, ticker]
                    display_volume = volume_df.loc[breakout_datetime, ticker]
                    display_total_volume = total_volume_df.loc[breakout_datetime, ticker]
                    display_additional_info = f'Previous new high close: {val_df.iloc[[-2]].loc[:, ticker].iat[0, 0]}' if (list_idx == 0) else f'Previous new high: {val_df.iloc[[-2]].loc[:, ticker].iat[0, 0]}, New high: {val_df.iloc[[-1]].loc[:, ticker].iat[0, 0]}'
                    
                    for ticker in ticker_list:
                        read_ticker_str = " ".join(ticker)
                        logger.log_debug_msg(f'{read_ticker_str} reaching {status} at {read_time_str}', with_speech = True)
                        logger.log_debug_msg(f'{ticker} reaching {status} at {display_time_str}, {display_additional_info}, Close: {display_close}, Close change: {display_close_pct}, Previous close: {display_previous_close}, Previous close change: {display_previous_close_pct_change}, Volume: {display_volume}, Total volume: {display_total_volume}', with_std_out = True)
        
        logger.log_debug_msg(f'Closest to new high or new high analysis time: {time.time() - start_time} seconds')
            
            
        
        
        
        