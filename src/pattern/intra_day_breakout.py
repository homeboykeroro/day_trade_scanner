import time
import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from model.discord.scanner_result_message import ScannerResultMessage

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.scatter_colour import ScatterColour
from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

from utils.chart_util import get_candlestick_chart
from utils.common.dataframe_util import concat_daily_df_and_minute_df, derive_idx_df
from utils.common.datetime_util import convert_into_human_readable_time, convert_into_read_out_time
from utils.logger import Logger
from utils.common.config_util import get_config

idx = pd.IndexSlice
logger = Logger()

SHOW_DISCORD_DEBUG_LOG = get_config('INTRA_DAY_BREAKOUT', 'SHOW_DISCORD_DEBUG_LOG')

class IntraDayBreakout(PatternAnalyser):    
    def __init__(self, bar_size: BarSize, minute_candle_df: DataFrame, daily_candle_df: DataFrame, 
                       ticker_to_contract_info_dict: dict, 
                       discord_client,
                       min_observe_period,
                       first_pop_up_min_close_pct,
                       min_breakout_trading_volume_in_usd,
                       daily_and_minute_candle_gap,
                       pattern_name):
        super().__init__(discord_client)
        self.__bar_size = bar_size
        self.__minute_candle_df = minute_candle_df
        
        self.__min_observe_period = min_observe_period
        self.__first_pop_up_min_close_pct = first_pop_up_min_close_pct
        self.__min_breakout_trading_volume_in_usd = min_breakout_trading_volume_in_usd
        self.__daily_and_minute_candle_gap = daily_and_minute_candle_gap
        self.__pattern_name = pattern_name
        
        minute_candle_ticker_list = list(minute_candle_df.columns.get_level_values(0).unique())
        daily_candle_df_ticker_list = daily_candle_df.columns.get_level_values(0).unique().tolist()
        select_daily_candle_df_ticker_list = []
        for ticker in minute_candle_ticker_list:
            if ticker in daily_candle_df_ticker_list:
                select_daily_candle_df_ticker_list.append(ticker)
        
        self.__daily_candle_df = daily_candle_df.loc[:, idx[select_daily_candle_df_ticker_list, :]]
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Intra day breakout scan')
        start_time = time.time()
        
        period = len(self.__minute_candle_df)
        if period < self.__min_observe_period:
            logger.log_debug_msg(f'{self.__bar_size} candle length of {len(self.__minute_candle_df)} is less than {self.__min_observe_period}', with_std_out=True)
            return
        
        close_df = (self.__minute_candle_df.loc[:, idx[:, Indicator.CLOSE.value]]
                                           .replace(np.nan, -1)
                                           .rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value}))
        high_df = (self.__minute_candle_df.loc[:, idx[:, Indicator.HIGH.value]]
                                          .replace(np.nan, -1)
                                          .rename(columns={Indicator.HIGH.value: RuntimeIndicator.COMPARE.value}))
        low_df = (self.__minute_candle_df.loc[:, idx[:, Indicator.LOW.value]]
                                         .replace(np.nan, -1)
                                         .rename(columns={Indicator.LOW.value: RuntimeIndicator.COMPARE.value}))
        volume_df = (self.__minute_candle_df.loc[:, idx[:, Indicator.VOLUME.value]]
                                            .replace(np.nan, -1)
                                            .rename(columns={Indicator.VOLUME.value: RuntimeIndicator.COMPARE.value}))
        
        # No way to get actual USD volume at specific time, so just take the average of high, low and close
        trading_volume_df = ((close_df + high_df + low_df).div(3)) * volume_df
        
        yesterday_daily_candle_df = self.__daily_candle_df.iloc[[-1]]
        yesterday_close_df = yesterday_daily_candle_df.loc[:, idx[:, Indicator.CLOSE.value]]
        yesterday_close_to_last_pct_df = (close_df.sub(yesterday_close_df.values)
                                                  .div(yesterday_close_df.values)
                                                  .mul(100))
        
        min_close_pct_boolean_df = (yesterday_close_to_last_pct_df >= self.__first_pop_up_min_close_pct)
        min_breakout_trading_volume_boolean_df = (trading_volume_df >= self.__min_breakout_trading_volume_in_usd)
        pop_up_boolean_df = (min_close_pct_boolean_df) & (min_breakout_trading_volume_boolean_df)
    
        datetime_idx_df = derive_idx_df(pop_up_boolean_df, False)
        first_pop_up_occurrence_df = datetime_idx_df.where(pop_up_boolean_df.values).bfill().iloc[[0]]
        
        pop_up_result_series = pop_up_boolean_df.any()   
        pop_up_ticker_list = pop_up_result_series.index[pop_up_result_series].get_level_values(0).tolist()    
              
        for ticker in pop_up_ticker_list:
            # Only highest close/ high with minimum USD trading volume will be considered as breakout
            close_with_min_breakout_volume_df = (close_df.where(min_breakout_trading_volume_boolean_df.values)
                                                         .replace(np.nan, -1)
                                                         .loc[:, idx[[ticker], :]])
            high_with_min_breakout_volume_df = (high_df.where(min_breakout_trading_volume_boolean_df.values)
                                                        .replace(np.nan, -1)
                                                        .loc[:, idx[[ticker], :]])

            sorted_breakout_high_idx_np = np.argsort(high_with_min_breakout_volume_df.values, axis=0)
            sorted_breakout_high_np = high_with_min_breakout_volume_df.values[sorted_breakout_high_idx_np, np.arange(high_with_min_breakout_volume_df.shape[1])]
            breakout_high_datetime = high_with_min_breakout_volume_df.index[sorted_breakout_high_idx_np[-1][0]]
            previous_breakout_high_datetime = high_with_min_breakout_volume_df.index[sorted_breakout_high_idx_np[-2][0]]
            breakout_close = sorted_breakout_close_np[-1][0]
            previous_breakout_close = sorted_breakout_close_np[-2][0]
            
            sorted_breakout_close_idx_np = np.argsort(close_with_min_breakout_volume_df.values, axis=0)
            sorted_breakout_close_np = close_with_min_breakout_volume_df.values[sorted_breakout_close_idx_np, np.arange(close_with_min_breakout_volume_df.shape[1])]
            breakout_close_datetime = close_with_min_breakout_volume_df.index[sorted_breakout_close_idx_np[-1][0]]
            previous_breakout_close_datetime = close_with_min_breakout_volume_df.index[sorted_breakout_close_idx_np[-2][0]]
            breakout_high = sorted_breakout_high_np[-1][0]
            previous_breakout_high = sorted_breakout_high_np[-2][0]
            
            breakout_indicator = None
            breakout_datetime = None
            breakout_value = None
            previous_breakout_datetime = None
            previous_breakout_value = None
            breakout_volume = volume_df.loc[breakout_datetime, (ticker, RuntimeIndicator.COMPARE.value)]
      
            # Case 1: Pop up candle (2nd high) -> Pullback -> Consolidation -> Curl (increasing volume) -> Breakout with volume)
            # Message precedence, breakout high > breakout close
            if previous_breakout_close != -1:
                breakout_indicator = Indicator.CLOSE.value
                breakout_value = breakout_close
                breakout_datetime = breakout_close_datetime
                previous_breakout_value = previous_breakout_close
                previous_breakout_datetime = previous_breakout_close_datetime
      
            if previous_breakout_high != -1:
                breakout_indicator = Indicator.HIGH.value
                breakout_value = breakout_high
                breakout_datetime = breakout_high_datetime
                previous_breakout_value = previous_breakout_high
                previous_breakout_datetime = previous_breakout_high_datetime
        
            if breakout_indicator == None:
                continue
        
            normalised_volume_df = (volume_df.where(min_breakout_trading_volume_boolean_df.values)
                                             .replace(np.nan, -1)
                                             .loc[:breakout_datetime, idx[[ticker], :]])
            breakout_volume = normalised_volume_df.loc[breakout_datetime, (ticker, RuntimeIndicator.COMPARE.value)]
            
            first_pop_up_datetime = first_pop_up_occurrence_df.loc[first_pop_up_occurrence_df.index[0], (ticker, RuntimeIndicator.INDEX.value)]
            
            check_message_sent_start_time = time.time()
            is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, 
                                                                          hit_scanner_datetime=breakout_datetime.replace(second=0, microsecond=0), 
                                                                          pattern=self.__pattern_name, 
                                                                          bar_size=self.__bar_size)
            logger.log_debug_msg(f'Check {ticker} intra day breakout message send time: {time.time() - check_message_sent_start_time} seconds')
            
            if not is_message_sent:
                # with pd.option_context('display.max_rows', None,
                #                        'display.max_columns', None,
                #                        'display.precision', 3):
                #     logger.log_debug_msg(f'{ticker} Intra Day Breakout Full Dataframe:')
                #     logger.log_debug_msg(self.__minute_candle_df.loc[:, idx[[ticker], :]]) 
                
                # logger.log_debug_msg(f'{ticker} breakout datetime: {breakout_datetime}, breakout value of {breakout_indicator}: ${breakout_value} \n previous high datetime: {previous_high_datetime}, previous high value: ${previous_high}')
                
                # if SHOW_DISCORD_DEBUG_LOG:
                #     self._discord_client.send_message(DiscordMessage(content=f'{ticker} breakout datetime: {breakout_datetime}, breakout value of {breakout_indicator}: ${breakout_value} \n previous high datetime: {previous_high_datetime}, previous high value: ${previous_high}, candlestick chart display start datetime: {candlestick_chart_display_start_datetime}, first valid volume datetime: {first_valid_volume_datetime}'), DiscordChannel.INTRA_DAY_BREAKOUT_LOG)
                
                candle_chart_data_df, daily_date_to_fake_minute_datetime_x_axis_dict = concat_daily_df_and_minute_df(daily_df=self.__daily_candle_df, 
                                                                                                                     minute_df=self.__minute_candle_df, 
                                                                                                                     hit_scanner_datetime=breakout_datetime,
                                                                                                                     select_datetime_start_range=first_pop_up_datetime,
                                                                                                                     gap_btw_daily_and_minute=self.__daily_and_minute_candle_gap)
                
                contract_info = self.__ticker_to_contract_info_dict[ticker]
                close = self.__minute_candle_df.loc[breakout_datetime, (ticker, Indicator.CLOSE.value)]
                high = self.__minute_candle_df.loc[breakout_datetime, (ticker, Indicator.HIGH.value)]
                yesterday_close = yesterday_close_df.loc[yesterday_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                yesterday_close_to_last_pct = round((((close - yesterday_close) / yesterday_close) * 100), 2) if breakout_indicator == Indicator.CLOSE.value else round((((high - yesterday_close) / yesterday_close) * 100), 2)

                total_volume = self.__minute_candle_df.loc[breakout_datetime, (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]
                
                candle_comment_list = [CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE, Indicator.CLOSE, Indicator.VOLUME]
                
                one_minute_chart_start_time = time.time()
                logger.log_debug_msg(f'Generate {ticker} intra day breakout one minute chart')
                chart_dir = get_candlestick_chart(candle_data_df=candle_chart_data_df,
                                                  ticker=ticker, pattern=self.__pattern_name, bar_size=self.__bar_size,
                                                  daily_date_to_fake_minute_datetime_x_axis_dict=daily_date_to_fake_minute_datetime_x_axis_dict,
                                                  hit_scanner_datetime=breakout_datetime,
                                                  symbol_position_list=[previous_breakout_datetime, breakout_datetime],
                                                  positive_offset=0, negative_offset=0,
                                                  scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.GREEN,
                                                  candle_comment_list=candle_comment_list)
                logger.log_debug_msg(f'Generate {ticker} intra day breakout {self.__bar_size.value} chart finished time: {time.time() - one_minute_chart_start_time} seconds')
                
                hit_scanner_datetime_display = convert_into_human_readable_time(breakout_datetime)
                previous_breakout_high_datetime_display = convert_into_human_readable_time(previous_breakout_high)
                read_out_dip_time = convert_into_read_out_time(breakout_datetime)
                    
                message = ScannerResultMessage(title=f'{ticker} {breakout_indicator} is breaking out {yesterday_close_to_last_pct}% at {hit_scanner_datetime_display}, breakout high: {breakout_value}, previous high: {previous_breakout_value} at {previous_breakout_high_datetime_display}, breakout volume: {"{:,}".format(int(breakout_volume))}',
                                               readout_msg=f'{" ".join(ticker)} {breakout_indicator} is breaking out {yesterday_close_to_last_pct}% at {read_out_dip_time}',
                                               close=close,
                                               yesterday_close=yesterday_close,
                                               volume=breakout_volume, total_volume=total_volume,
                                               contract_info=contract_info,
                                               chart_dir=chart_dir,
                                               ticker=ticker,
                                               hit_scanner_datetime=breakout_datetime.replace(second=0, microsecond=0),
                                               pattern=self.__pattern_name,
                                               bar_size=self.__bar_size.value)
                message_list.append(message)
            
            if message_list:
                send_msg_start_time = time.time()
                self.send_notification(message_list, DiscordChannel.INTRA_DAY_BREAKOUT)
                logger.log_debug_msg(f'{self.__pattern_name} send message time: {time.time() - send_msg_start_time} seconds')