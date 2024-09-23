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
from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

from utils.chart_util import get_candlestick_chart
from utils.dataframe_util import concat_daily_df_and_minute_df, derive_idx_df
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time
from utils.logger import Logger
from utils.config_util import get_config

idx = pd.IndexSlice
logger = Logger()

PATTERN_NAME = 'INTRA_DAY_BREAKOUT'

SHOW_DISCORD_DEBUG_LOG = get_config('INTRA_DAY_BREAKOUT_PARAM', 'SHOW_DISCORD_DEBUG_LOG')

MIN_OBSERVE_PERIOD = get_config('INTRA_DAY_BREAKOUT_PARAM', 'MIN_OBSERVE_PERIOD')
TOP_N_VOLUME = get_config('INTRA_DAY_BREAKOUT_PARAM', 'TOP_N_VOLUME')
MIN_BREAKOUT_TRADING_VOLUME_IN_USD = get_config('INTRA_DAY_BREAKOUT_PARAM', 'MIN_BREAKOUT_TRADING_VOLUME_IN_USD')
DAILY_AND_MINUTE_CANDLE_GAP = get_config('INTRA_DAY_BREAKOUT_PARAM', 'DAILY_AND_MINUTE_CANDLE_GAP')
MIN_VALID_CANDLESTICK_CHART_DISPLAY_VOLUME = get_config('INTRA_DAY_BREAKOUT_PARAM', 'MIN_VALID_CANDLESTICK_CHART_DISPLAY_VOLUME')

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
        
        self.__ticker_list = select_daily_df_ticker_list
        self.__daily_df = daily_df.loc[:, idx[select_daily_df_ticker_list, :]]
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Intra day breakout scan')
        start_time = time.time()
        
        period = len(self.__historical_data_df)
        if period < MIN_OBSERVE_PERIOD:
            return
        
        close_df = (self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
                                             .rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value}))
        high_df = (self.__historical_data_df.loc[:, idx[:, Indicator.HIGH.value]]
                                            .rename(columns={Indicator.HIGH.value: RuntimeIndicator.COMPARE.value}))
        volume_df = (self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME.value]]
                                              .rename(columns={Indicator.VOLUME.value: RuntimeIndicator.COMPARE.value}))
        trading_volume_df = close_df * volume_df
        min_breakout_trading_volume_boolean_df = (trading_volume_df >= MIN_BREAKOUT_TRADING_VOLUME_IN_USD)
        
        select_valid_volume_candlestick_display_boolean_df = trading_volume_df >= MIN_VALID_CANDLESTICK_CHART_DISPLAY_VOLUME
        datetime_idx_df = derive_idx_df(select_valid_volume_candlestick_display_boolean_df, False)
        first_valid_volume_datetime_df = (datetime_idx_df.where(select_valid_volume_candlestick_display_boolean_df.values)
                                                         .bfill()
                                                         .iloc[[0]])
        
        for ticker in self.__ticker_list:
            min_breakout_volume_close_df = (close_df.where(min_breakout_trading_volume_boolean_df.values)
                                                    .replace(np.nan, -1)
                                                    .loc[:, idx[[ticker], :]])
            min_breakout_volume_high_df = (high_df.where(min_breakout_trading_volume_boolean_df.values)
                                                  .replace(np.nan, -1)
                                                  .loc[:, idx[[ticker], :]])

            sorted_breakout_close_idx_np = np.argsort(min_breakout_volume_close_df.values, axis=0)
            sorted_breakout_close_np = min_breakout_volume_close_df.values[sorted_breakout_close_idx_np, np.arange(min_breakout_volume_close_df.shape[1])]
            
            sorted_breakout_high_idx_np = np.argsort(min_breakout_volume_high_df.values, axis=0)
            sorted_breakout_high_np = min_breakout_volume_high_df.values[sorted_breakout_high_idx_np, np.arange(min_breakout_volume_high_df.shape[1])]
            
            breakout_close = sorted_breakout_close_np[-1][0]
            breakout_high = sorted_breakout_high_np[-1][0]
            
            #Not Enough Volume for Breakout
            if breakout_close == -1 and breakout_high == -1:
                continue
            
            breakout_indicator = None
            breakout_value = None
            previous_high = None
            previous_high_datetime = None
            first_valid_volume_datetime = None
            candlestick_chart_display_start_datetime = None
            breakout_close_datetime = min_breakout_volume_close_df.index[sorted_breakout_close_idx_np[-1][0]]
            breakout_high_datetime = min_breakout_volume_high_df.index[sorted_breakout_high_idx_np[-1][0]]
    
            if breakout_high != -1:
                normalised_high_df = (high_df.replace(np.nan, -1)
                                             .loc[:breakout_high_datetime, idx[[ticker], :]])
                
                sorted_high_idx_np = np.argsort(normalised_high_df.values, axis=0)
                sorted_high_np = normalised_high_df.values[sorted_high_idx_np, np.arange(normalised_high_df.shape[1])]

                # first breakout occurrence (high)
                for high_idx in sorted_breakout_high_idx_np:
                    high_idx = high_idx[0]
                    high_val = min_breakout_volume_high_df.loc[min_breakout_volume_high_df.index[high_idx], (ticker, RuntimeIndicator.COMPARE.value)]
                
                    if high_val != -1:
                        high_datetime = min_breakout_volume_high_df.index[high_idx]
                        candlestick_chart_display_start_datetime = high_datetime
                        break
                
                if (candlestick_chart_display_start_datetime is None 
                        or candlestick_chart_display_start_datetime == breakout_high_datetime):
                    first_valid_volume_datetime = first_valid_volume_datetime_df.loc[first_valid_volume_datetime_df.index[0], (ticker, RuntimeIndicator.INDEX.value)]
                    
                    if candlestick_chart_display_start_datetime > first_valid_volume_datetime:
                        candlestick_chart_display_start_datetime = first_valid_volume_datetime
                
                if len(sorted_high_np) >= 2:
                    if sorted_high_np[-2][0] != -1:
                        breakout_indicator = Indicator.HIGH.value
                        breakout_value = breakout_high
                        breakout_datetime = breakout_high_datetime
                        
                        previous_high = sorted_high_np[-2][0]
                        previous_high_datetime = normalised_high_df.index[sorted_high_idx_np[-2][0]]
                    else:
                        breakout_value = None
                        breakout_datetime = breakout_high_datetime
                        previous_high = None
                        previous_high_datetime = None
                else:
                    breakout_value = breakout_high
                    breakout_datetime = breakout_high_datetime
                    previous_high = None
                    previous_high_datetime = None
                
            #Intra Day Breakout Message Precedence, Breakout Close > Breakout High
            if breakout_value is None or previous_high is None:
                normalised_close_df = (close_df.replace(np.nan, -1)
                                               .loc[:breakout_close_datetime, idx[[ticker], :]])
                
                sorted_close_idx_np = np.argsort(normalised_close_df.values, axis=0)
                sorted_close_np = normalised_close_df.values[sorted_close_idx_np, np.arange(normalised_close_df.shape[1])]
                
                # first breakout occurrence (close)
                for close_idx in sorted_breakout_close_idx_np:
                    close_idx = close_idx[0]
                    close_val = min_breakout_volume_close_df.loc[min_breakout_volume_close_df.index[close_idx], (ticker, RuntimeIndicator.COMPARE.value)]
                
                    if close_val != -1:
                        close_datetime = min_breakout_volume_close_df.index[close_idx]
                        candlestick_chart_display_start_datetime = close_datetime
                        break
                
                if (candlestick_chart_display_start_datetime is None 
                        or candlestick_chart_display_start_datetime == breakout_close_datetime):
                    first_valid_volume_datetime = first_valid_volume_datetime_df.loc[first_valid_volume_datetime_df.index[0], (ticker, RuntimeIndicator.INDEX.value)]
                    
                    if candlestick_chart_display_start_datetime > first_valid_volume_datetime:
                        candlestick_chart_display_start_datetime = first_valid_volume_datetime
                
                if len(sorted_close_np) >= 2:
                    if sorted_close_np[-2][0] != -1:
                        breakout_indicator = Indicator.CLOSE.value
                        breakout_value = breakout_close
                        breakout_datetime = breakout_close_datetime

                        previous_high = sorted_close_np[-2][0]
                        previous_high_datetime = normalised_close_df.index[sorted_close_idx_np[-2][0]]
                    else:
                        breakout_value = None
                        breakout_datetime = breakout_close_datetime
                        previous_high = None
                        previous_high_datetime = None
                else:
                    breakout_value = breakout_close
                    breakout_datetime = breakout_close_datetime
                    previous_high = None
                    previous_high_datetime = None
            
            if breakout_value is None:
                continue
            
            normalised_volume_df = (volume_df.where(min_breakout_trading_volume_boolean_df.values)
                                             .replace(np.nan, -1)
                                             .loc[:breakout_datetime, idx[[ticker], :]])
            sorted_volume_idx_np = np.argsort(normalised_volume_df.values, axis=0)
            sorted_volume_np = normalised_volume_df.values[sorted_volume_idx_np, np.arange(normalised_volume_df.shape[1])]
            breakout_volume = normalised_volume_df.loc[breakout_datetime, (ticker, RuntimeIndicator.COMPARE.value)]
            top_n_volume_list = []
            
            is_top_n_volume = False
            
            for pos, volume_arry in enumerate(sorted_volume_np[-TOP_N_VOLUME:]):
                top_n_volume = volume_arry[0]
                
                if top_n_volume != -1:
                    top_n_volume_list.append(top_n_volume)
            
            for top_n_volume in top_n_volume_list:
                if breakout_volume >= top_n_volume:
                    is_top_n_volume = True
            
            if not is_top_n_volume:
                continue
            
            check_message_sent_start_time = time.time()
            is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, hit_scanner_datetime=breakout_datetime.replace(second=0, microsecond=0), pattern=PATTERN_NAME, bar_size=self.__bar_size)
            logger.log_debug_msg(f'Check {ticker} intra day breakout message send time: {time.time() - check_message_sent_start_time} seconds')
            
            if not is_message_sent:
                with pd.option_context('display.max_rows', None,
                                       'display.max_columns', None,
                                    'display.precision', 3):
                    logger.log_debug_msg(f'{ticker} Intra Day Breakout Full Dataframe:')
                    logger.log_debug_msg(self.__historical_data_df.loc[:, idx[[ticker], :]]) 
                
                logger.log_debug_msg(f'{ticker} breakout datetime: {breakout_datetime}, breakout value of {breakout_indicator}: ${breakout_value} \n previous high datetime: {previous_high_datetime}, previous high value: ${previous_high}')
                
                if SHOW_DISCORD_DEBUG_LOG:
                    self._discord_client.send_message(DiscordMessage(content=f'{ticker} breakout datetime: {breakout_datetime}, breakout value of {breakout_indicator}: ${breakout_value} \n previous high datetime: {previous_high_datetime}, previous high value: ${previous_high}, candlestick chart display start datetime: {candlestick_chart_display_start_datetime}, first valid volume datetime: {first_valid_volume_datetime}'), DiscordChannel.INTRA_DAY_BREAKOUT_LOG)
                
                candle_chart_data_df, daily_date_to_fake_minute_datetime_x_axis_dict = concat_daily_df_and_minute_df(daily_df=self.__daily_df, 
                                                                                                                     minute_df=self.__historical_data_df, 
                                                                                                                     hit_scanner_datetime=breakout_datetime,
                                                                                                                     is_hit_scanner_datetime_start_range=False,
                                                                                                                     gap_btw_daily_and_minute=DAILY_AND_MINUTE_CANDLE_GAP)
                
                contract_info = self.__ticker_to_contract_info_dict[ticker]
                close = self.__historical_data_df.loc[breakout_datetime, (ticker, Indicator.CLOSE.value)]
                yesterday_close = self.__daily_df.loc[self.__daily_df.index[-1], (ticker, Indicator.CLOSE.value)]
                yesterday_close_to_last_pct = round((((close - yesterday_close) / yesterday_close) * 100), 2)

                total_volume = self.__historical_data_df.loc[breakout_datetime, (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]
                
                minute_candle_negative_offset = int(((breakout_datetime - candlestick_chart_display_start_datetime).total_seconds() / 60)) + len(self.__daily_df)
                
                candle_comment_list = [CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE, Indicator.CLOSE, Indicator.VOLUME]
                
                one_minute_chart_start_time = time.time()
                logger.log_debug_msg(f'Generate {ticker} intra day breakout one minute chart')
                chart_dir = get_candlestick_chart(candle_data_df=candle_chart_data_df,
                                                  ticker=ticker, pattern=PATTERN_NAME, bar_size=self.__bar_size,
                                                  daily_date_to_fake_minute_datetime_x_axis_dict=daily_date_to_fake_minute_datetime_x_axis_dict,
                                                  hit_scanner_datetime=breakout_datetime,
                                                  positive_offset=0, negative_offset=minute_candle_negative_offset,
                                                  scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.GREEN,
                                                  candle_comment_list=candle_comment_list)
                logger.log_debug_msg(f'Generate {ticker} intra day breakout one minute chart finished time: {time.time() - one_minute_chart_start_time} seconds')
                
                hit_scanner_datetime_display = convert_into_human_readable_time(breakout_datetime)
                read_out_dip_time = convert_into_read_out_time(breakout_datetime)
                    
                append_previous_high_display = f'${previous_high}' if previous_high is not None else 'None'
                append_previous_high_datetime_display = f'at {previous_high_datetime.strftime(("%Y-%m-%d %H:%M"))}' if previous_high_datetime is not None else 'at None'
                message = ScannerResultMessage(title=f'{ticker} is breaking out {yesterday_close_to_last_pct}% at {hit_scanner_datetime_display}, breaking high: ${breakout_value} at {breakout_datetime.strftime(("%Y-%m-%d %H:%M"))}, previous high: {append_previous_high_display} {append_previous_high_datetime_display}, breakout volume: {"{:,}".format(int(breakout_volume))}',
                                               readout_msg=f'{" ".join(ticker)} is breaking out {yesterday_close_to_last_pct}% at {read_out_dip_time}',
                                               close=close,
                                               yesterday_close=yesterday_close,
                                               volume=breakout_volume, total_volume=total_volume,
                                               contract_info=contract_info,
                                               chart_dir=chart_dir,
                                               ticker=ticker,
                                               hit_scanner_datetime=breakout_datetime.replace(second=0, microsecond=0),
                                               pattern=PATTERN_NAME,
                                               bar_size=self.__bar_size.value)
                message_list.append(message)
            
            if message_list:
                send_msg_start_time = time.time()
                self.send_notification(message_list, DiscordChannel.INTRA_DAY_BREAKOUT)
                logger.log_debug_msg(f'{PATTERN_NAME} send message time: {time.time() - send_msg_start_time} seconds')