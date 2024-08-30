import math
import time
import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from model.discord.scanner_result_message import ScannerResultMessage
from model.discord.discord_message import DiscordMessage

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.scatter_colour import ScatterColour
from constant.candle.candle_colour import CandleColour
from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

from utils.chart_util import get_candlestick_chart
from utils.dataframe_util import derive_idx_df, replace_daily_df_latest_day_with_minute, get_ticker_to_occurrence_idx_list
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time, get_current_us_datetime
from utils.logger import Logger
from utils.config_util import get_config

idx = pd.IndexSlice
logger = Logger()

PATTERN_NAME = 'INTRA_DAY_BREAKOUT'

MIN_OBSERVE_PERIOD = get_config('INTRA_DAY_BREAKOUT_PARAM', 'MIN_OBSERVE_PERIOD')
TOP_N_VOLUME = get_config('INTRA_DAY_BREAKOUT_PARAM', 'TOP_N_VOLUME')
TOP_N_VOLUME_MIN_TRADING_VOLUME_IN_USD = get_config('INTRA_DAY_BREAKOUT_PARAM', 'TOP_N_VOLUME_MIN_TRADING_VOLUME_IN_USD')
IGNORE_COMMENT_BAR_LENGTH  = get_config('INTRA_DAY_BREAKOUT_PARAM', 'IGNORE_COMMENT_BAR_LENGTH')
MIN_BREAKOUT_TRADING_VOLUME_IN_USD = get_config('INTRA_DAY_BREAKOUT_PARAM', 'MIN_BREAKOUT_TRADING_VOLUME_IN_USD')
MIN_GAP_UP_PCT = get_config('INITIAL_POP_PARAM', 'MIN_GAP_UP_PCT')
MIN_YESTERDAY_CLOSE_TO_LAST_PCT = get_config('INITIAL_POP_PARAM', 'MIN_YESTERDAY_CLOSE_TO_LAST_PCT')

class IntraDayBreakout(PatternAnalyser):    
    def __init__(self, bar_size: BarSize, historical_data_df: DataFrame, daily_df: DataFrame, ticker_to_contract_info_dict: dict, discord_client):
        ticker_list = list(historical_data_df.columns.get_level_values(0).unique())
        super().__init__(discord_client)
        self.__bar_size = bar_size
        self.__historical_data_df = historical_data_df
        
        daily_df_ticker_list = daily_df.columns.get_level_values(0).unique().tolist()
        select_daily_df_ticker_list = []
        for ticker in ticker_list:
            if ticker in daily_df_ticker_list:
                select_daily_df_ticker_list.append(ticker)
        
        self.__daily_df = daily_df.loc[:, idx[select_daily_df_ticker_list, :]]
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Intra day breakout scan')
        start_time = time.time()
        
        candle_colour_df = (self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
                                    .rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value}))
        period = len(self.__historical_data_df)
        
        close_df = (self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
                            .rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value}))
        max_close_list = close_df.max().values
        second_largest_close_idx_list = np.argsort(close_df.values, axis=0)[-2, :]
        second_largest_close_datetime_idx_df = pd.DataFrame([close_df.index[second_largest_close_idx_list]], columns=close_df.columns)
        
        high_df = (self.__historical_data_df.loc[:, idx[:, Indicator.HIGH.value]]
                            .rename(columns={Indicator.HIGH.value: RuntimeIndicator.COMPARE.value}))
        max_high_list = high_df.max().values
        second_largest_high_idx_list = np.argsort(high_df.values, axis=0)[-2, :]
        second_largest_high_datetime_idx_df = pd.DataFrame([high_df.index[second_largest_high_idx_list]], columns=high_df.columns)
        
        volume_df = (self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME.value]]
                            .rename(columns={Indicator.VOLUME.value: RuntimeIndicator.COMPARE.value}))
        
        yesterday_daily_candle_df = self.__daily_df.iloc[[-1]]
        yesterday_close_df = yesterday_daily_candle_df.loc[:, idx[:, Indicator.CLOSE.value]]
        yesterday_close_to_last_pct_df = (close_df.sub(yesterday_close_df.values)
                                                  .div(yesterday_close_df.values)
                                                  .mul(100))
        
        
        if period >= MIN_OBSERVE_PERIOD:
            n = TOP_N_VOLUME if len(volume_df) >= TOP_N_VOLUME else MIN_OBSERVE_PERIOD
            non_none_and_zero_min_volume_df = (volume_df.where(((volume_df * close_df) >= TOP_N_VOLUME_MIN_TRADING_VOLUME_IN_USD).values))
            top_n_volume_check_boolean_df = (non_none_and_zero_min_volume_df.iloc[[-1]].reset_index(drop=True) == non_none_and_zero_min_volume_df.max().values)
            top_n_volume_idx_np = np.argsort(non_none_and_zero_min_volume_df.values, axis=0)
            
            for n in range(TOP_N_VOLUME):
                top_n_volume_idx_list = top_n_volume_idx_np[-n]
                top_n_volume_list = non_none_and_zero_min_volume_df.values[top_n_volume_idx_list, np.arange(non_none_and_zero_min_volume_df.shape[1])]

                volume_check_boolean_df = ((non_none_and_zero_min_volume_df.iloc[[-1]].reset_index(drop=True) >= top_n_volume_list) 
                                                & (non_none_and_zero_min_volume_df.iloc[[-1]].reset_index(drop=True) != -1))
                top_n_volume_check_boolean_df = top_n_volume_check_boolean_df | volume_check_boolean_df
                
            breakout_with_huge_vol_and_close_boolean_df = (top_n_volume_check_boolean_df
                                                            & (close_df.iloc[[-1]].reset_index(drop=True) == max_close_list) 
                                                            & (candle_colour_df.iloc[[-1]].reset_index(drop=True) != CandleColour.GREY.value) 
                                                            & ((non_none_and_zero_min_volume_df * close_df).iloc[[-1]].reset_index(drop=True) >= MIN_BREAKOUT_TRADING_VOLUME_IN_USD))
            breakout_with_huge_vol_and_high_boolean_df = (top_n_volume_check_boolean_df
                                                            & (high_df.iloc[[-1]].reset_index(drop=True) == max_high_list) 
                                                            & (candle_colour_df.iloc[[-1]].reset_index(drop=True) != CandleColour.GREY.value) 
                                                            & ((non_none_and_zero_min_volume_df * close_df).iloc[[-1]].reset_index(drop=True) >= MIN_BREAKOUT_TRADING_VOLUME_IN_USD))
            
            breakout_with_huge_vol_and_close_result_series = breakout_with_huge_vol_and_close_boolean_df.any()
            breakout_with_huge_vol_and_high_result_series = breakout_with_huge_vol_and_high_boolean_df.any()
            
            huge_vol_and_close_result_list = breakout_with_huge_vol_and_close_result_series.index[breakout_with_huge_vol_and_close_result_series].get_level_values(0).tolist() 
            huge_vol_and_high_result_list = breakout_with_huge_vol_and_high_result_series.index[breakout_with_huge_vol_and_high_result_series].get_level_values(0).tolist() 
          
            ticker_list = list(set(huge_vol_and_close_result_list + huge_vol_and_high_result_list))
            last_candle_datetime = volume_df.index[-1]
            logger.log_debug_msg(f'Intra day breakout analysis time: {time.time() - start_time} seconds')
            
            for ticker in ticker_list:
                check_message_sent_start_time = time.time()
                is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, hit_scanner_datetime=last_candle_datetime.replace(second=0, microsecond=0), pattern=PATTERN_NAME, bar_size=self.__bar_size)
                logger.log_debug_msg(f'Check {ticker} intra day breakout message send time: {time.time() - check_message_sent_start_time} seconds')

                if not is_message_sent:
                    with pd.option_context('display.max_rows', None,
                                           'display.max_columns', None,
                                        'display.precision', 3):
                        logger.log_debug_msg(f'{ticker} Intra Day Breakout Full Dataframe:')
                        logger.log_debug_msg(self.__historical_data_df.loc[:, idx[[ticker], :]]) 
                    
                    contract_info = self.__ticker_to_contract_info_dict[ticker]
                    yesterday_close_to_last_pct = yesterday_close_to_last_pct_df.loc[last_candle_datetime, (ticker, RuntimeIndicator.COMPARE.value)]
                    close = self.__historical_data_df.loc[last_candle_datetime, (ticker, Indicator.CLOSE.value)]
                    yesterday_close = yesterday_close_df.loc[yesterday_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                    volume = self.__historical_data_df.loc[last_candle_datetime, (ticker, Indicator.VOLUME.value)]
                    total_volume = self.__historical_data_df.loc[last_candle_datetime, (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]
                    
                    if ticker in huge_vol_and_close_result_list:
                        breakout_indicator = Indicator.CLOSE.value
                        breakout_value = close_df.loc[:, idx[ticker, :]].max()[(ticker, RuntimeIndicator.COMPARE)]
                        previous_high_value_np = close_df.values[second_largest_close_idx_list, np.arange(close_df.shape[1])]
                        previous_high_df = pd.DataFrame([previous_high_value_np], columns=close_df.columns)
                        previous_high = previous_high_df.loc[previous_high_df.index[-1], (ticker, RuntimeIndicator.COMPARE)]
                        previous_high_datetime = second_largest_close_datetime_idx_df.loc[second_largest_close_datetime_idx_df.index[-1], (ticker, RuntimeIndicator.COMPARE.value)]
                        
                    if ticker in huge_vol_and_high_result_list:
                        breakout_indicator = Indicator.HIGH.value
                        breakout_value = high_df.loc[:, idx[ticker, :]].max()[(ticker, RuntimeIndicator.COMPARE)]
                        previous_high_value_np = high_df.values[second_largest_high_idx_list, np.arange(high_df.shape[1])]
                        previous_high_df = pd.DataFrame([previous_high_value_np], columns=high_df.columns)
                        previous_high = previous_high_df.loc[previous_high_df.index[-1], (ticker, RuntimeIndicator.COMPARE)]
                        previous_high_datetime = second_largest_high_datetime_idx_df.loc[second_largest_high_datetime_idx_df.index[-1], (ticker, RuntimeIndicator.COMPARE.value)]
                        
                    daily_df = replace_daily_df_latest_day_with_minute(daily_df=self.__daily_df.loc[:, idx[[ticker], :]], 
                                                                       minute_df=self.__historical_data_df.loc[[last_candle_datetime], idx[[ticker], :]])
                    minute_candle_negative_offset = math.ceil(((last_candle_datetime - previous_high_datetime).total_seconds() / 60))
                    logger.log_debug_msg(f'{ticker} last candle datetime: {last_candle_datetime}, breakout value of {breakout_indicator}: ${breakout_value} \n previous high datetime: {previous_high_datetime}, previous high value: ${previous_high} \n negative candle offset: {minute_candle_negative_offset}')
                    
                    self._discord_client.send_message(DiscordMessage(content=f'{ticker} last candle datetime: {last_candle_datetime}, breakout value of ${breakout_indicator}: {breakout_value} \n previous high datetime: {previous_high_datetime}, previous high value: ${previous_high} \n negative candle offset: {minute_candle_negative_offset}'), DiscordChannel.INTRA_DAY_BREAKOUT_LOG)
                    candle_comment_list = [CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE, Indicator.CLOSE, Indicator.VOLUME]
                    
                    if minute_candle_negative_offset >= IGNORE_COMMENT_BAR_LENGTH:
                        candle_comment_list = []
                    
                    one_minute_chart_start_time = time.time()
                    logger.log_debug_msg(f'Generate {ticker} intra day breakout one minute chart')
                    minute_chart_dir = get_candlestick_chart(candle_data_df=self.__historical_data_df,
                                                             ticker=ticker, pattern=PATTERN_NAME, bar_size=self.__bar_size,
                                                             hit_scanner_datetime=last_candle_datetime,
                                                             positive_offset=0, negative_offset=minute_candle_negative_offset,
                                                             scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.GREEN,
                                                             candle_comment_list=candle_comment_list)
                    logger.log_debug_msg(f'Generate {ticker} intra day breakout one minute chart finished time: {time.time() - one_minute_chart_start_time} seconds')
                        
                    daily_chart_start_time = time.time()
                    logger.log_debug_msg(f'Generate {ticker} intra day breakout daily chart')
                    daily_chart_dir = get_candlestick_chart(candle_data_df=daily_df,
                                                            ticker=ticker, pattern=PATTERN_NAME, bar_size=BarSize.ONE_DAY,
                                                            hit_scanner_datetime=daily_df.index[-1],
                                                            scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.GREEN)
                    logger.log_debug_msg(f'Generate {ticker} intra day breakout daily chart finished time: {time.time() - daily_chart_start_time} seconds')
                        
                    hit_scanner_datetime_display = convert_into_human_readable_time(last_candle_datetime)
                    read_out_dip_time = convert_into_read_out_time(last_candle_datetime)
                        
                    message = ScannerResultMessage(title=f'{ticker} is breaking out {round(yesterday_close_to_last_pct, 2)}% at {hit_scanner_datetime_display}, breaking high: ${breakout_value}, previous high: ${previous_high}',
                                                   readout_msg=f'{" ".join(ticker)} is breaking out {round(yesterday_close_to_last_pct, 2)}% at {read_out_dip_time}',
                                                   close=close,
                                                   yesterday_close=yesterday_close,
                                                   volume=volume, total_volume=total_volume,
                                                   contract_info=contract_info,
                                                   minute_chart_dir=minute_chart_dir,
                                                   daily_chart_dir=daily_chart_dir, 
                                                   ticker=ticker,
                                                   hit_scanner_datetime=last_candle_datetime.replace(second=0, microsecond=0),
                                                   pattern=PATTERN_NAME,
                                                   bar_size=self.__bar_size.value)
                    message_list.append(message)

            if message_list:
                send_msg_start_time = time.time()
                self.send_notification(message_list, DiscordChannel.INTRA_DAY_BREAKOUT, False)
                logger.log_debug_msg(f'{PATTERN_NAME} send message time: {time.time() - send_msg_start_time} seconds')