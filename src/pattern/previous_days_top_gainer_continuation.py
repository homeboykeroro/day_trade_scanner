import math
import time
import pandas as pd

from pattern.pattern_analyser import PatternAnalyser

from utils.common.datetime_util import convert_into_human_readable_time, convert_into_read_out_time, get_current_us_datetime
from utils.common.dataframe_util import concat_daily_df_and_minute_df, get_ticker_to_occurrence_idx_list
from utils.chart_util import get_candlestick_chart
from utils.common.config_util import get_config
from utils.logger import Logger

from model.discord.scanner_result_message import ScannerResultMessage
from model.discord.discord_message import DiscordMessage

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour
from constant.candle.bar_size import BarSize
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.scatter_colour import ScatterColour
from constant.discord.discord_channel import DiscordChannel

idx = pd.IndexSlice
logger = Logger()

PATTERN_NAME = 'PREVIOUS_DAY_TOP_GAINER_CONTINUATION'

SHOW_DISCORD_DEBUG_LOG = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'SHOW_DISCORD_DEBUG_LOG')

MIN_MULTI_DAYS_CLOSE_CHANGE_PCT = get_config('MULTI_DAYS_TOP_GAINER_SCAN_PARAM', 'MIN_MULTI_DAYS_CLOSE_CHANGE_PCT')
MAX_TOLERANCE_PERIOD_IN_MINUTE = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'MAX_TOLERANCE_PERIOD_IN_MINUTE')
MAX_NO_OF_ALERT_TIMES = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'MAX_NO_OF_ALERT_TIMES')
MIN_ALERT_CHECKING_INTERVAL_IN_MINUTE = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'MIN_ALERT_CHECKING_INTERVAL_IN_MINUTE')
MAX_CURRENT_DATETIME_AND_ALERT_DATETIME_DIFF_IN_MINUTE = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'MAX_CURRENT_DATETIME_AND_ALERT_DATETIME_DIFF_IN_MINUTE')
TEST_NEW_HIGH_TOLERANCE = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'TEST_NEW_HIGH_TOLERANCE')
GAP_UP_PCT = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'GAP_UP_PCT')
TOP_N_GAP_UP_OCCURRENCE_TIMES = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'TOP_N_GAP_UP_OCCURRENCE_TIMES')
DAILY_AND_MINUTE_CANDLE_GAP = get_config('PREVIOUS_DAY_TOP_GAINER_CONTINUATION_PARAM', 'DAILY_AND_MINUTE_CANDLE_GAP')

class PreviousDayTopGainerContinuation(PatternAnalyser): 
    def __init__(self, daily_df: pd.DataFrame, minute_df: pd.DataFrame, ticker_to_contract_info_dict: dict, discord_client):
        super().__init__(discord_client)
        self.__daily_df = daily_df
        self.__minute_df = minute_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict
    
    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Previous day top gainers continuation scan')

        daily_close_df = self.__daily_df.loc[:, idx[:, Indicator.CLOSE.value]].apply(pd.to_numeric, errors='coerce')
        daily_high_df = self.__daily_df.loc[:, idx[:, Indicator.HIGH.value]].apply(pd.to_numeric, errors='coerce')
        daily_close_pct_df = self.__daily_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]].apply(pd.to_numeric, errors='coerce')
        daily_volume_df = self.__daily_df.loc[:, idx[:, Indicator.VOLUME.value]].apply(pd.to_numeric, errors='coerce')
        daily_candle_colour_df = self.__daily_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
        
        max_daily_close_pct_boolean_series = (daily_close_pct_df.max() >= MIN_MULTI_DAYS_CLOSE_CHANGE_PCT).rename(index={CustomisedIndicator.CLOSE_CHANGE.value : RuntimeIndicator.COMPARE.value})
        max_daily_volume_dt_index_series = daily_volume_df.idxmax().rename(index={Indicator.VOLUME.value: RuntimeIndicator.COMPARE.value})
        max_daily_close_pct_dt_index_series = daily_close_pct_df.idxmax().rename(index={CustomisedIndicator.CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
        
        max_daily_close_dt_index = daily_close_df.idxmax().rename(index={Indicator.CLOSE.value : RuntimeIndicator.COMPARE.value})
        max_daily_high_dt_index = daily_high_df.idxmax().rename(index={Indicator.HIGH.value : RuntimeIndicator.COMPARE.value})
        is_ramp_up_candle_resistence_boolean_series = (max_daily_close_dt_index == max_daily_volume_dt_index_series) | (max_daily_high_dt_index == max_daily_volume_dt_index_series)
        
        max_daily_close_with_most_volume_ticker_series = (max_daily_volume_dt_index_series == max_daily_close_pct_dt_index_series) & (max_daily_close_pct_boolean_series) & (is_ramp_up_candle_resistence_boolean_series)
        previous_day_top_gainer_ticker_list = max_daily_close_with_most_volume_ticker_series.index[max_daily_close_with_most_volume_ticker_series].get_level_values(0).tolist()
        
        latest_daily_candle_date = self.__daily_df.index[-1]
        
        for ticker in previous_day_top_gainer_ticker_list:
            with pd.option_context('display.max_rows', None,
                                               'display.max_columns', None,
                                            'display.precision', 3):
                logger.log_debug_msg(f'{ticker} Continuation Daily Dataframe:')
                logger.log_debug_msg(self.__daily_df)
                logger.log_debug_msg(f'{ticker} Continuation Minute Dataframe:')
                logger.log_debug_msg(self.__minute_df)
            
            minute_df_ticker_list = self.__minute_df.columns.get_level_values(0).unique().tolist()
            daily_df_ticker_list = self.__daily_df.columns.get_level_values(0).unique().tolist()
            
            minute_data_not_found = ticker not in minute_df_ticker_list
            daily_data_not_found = ticker not in daily_df_ticker_list
            
            if minute_data_not_found or daily_data_not_found:
                concat_msg = ''
                if minute_data_not_found:
                    concat_msg += f'Minute candle data not found for {ticker}\n'
                
                if daily_data_not_found:
                    concat_msg += f'Daily candle data not found for {ticker}\n'
                    
                logger.log_debug_msg(f'{PATTERN_NAME} invalid dataframe found: \n{concat_msg}')    
                
                if concat_msg and SHOW_DISCORD_DEBUG_LOG:
                    self._discord_client.send_message(DiscordMessage(content=concat_msg), DiscordChannel.PREVIOUS_DAYS_TOP_GAINERS_CONTINUATION_DATA_NOT_FOUND_LOG)
                continue
            
            ramp_up_candle_date = max_daily_volume_dt_index_series[(ticker, RuntimeIndicator.COMPARE.value)]
            candle_colour = daily_candle_colour_df.loc[ramp_up_candle_date, (ticker, CustomisedIndicator.CANDLE_COLOUR.value)]
            yesterday_close = self.__daily_df.loc[latest_daily_candle_date, (ticker, Indicator.CLOSE.value)]
            
            is_green = candle_colour == CandleColour.GREEN.value
            ticker_minute_candle_df = self.__minute_df.loc[:, idx[ticker, :]]
            
            if is_green:
                # Previous day resistence
                ramp_up_close = self.__daily_df.loc[ramp_up_candle_date, (ticker, Indicator.CLOSE.value)]
                ramp_up_high = self.__daily_df.loc[ramp_up_candle_date, (ticker, Indicator.HIGH.value)]
                
                # Minute candle
                minute_high_df = ticker_minute_candle_df.loc[:, idx[:, Indicator.HIGH.value]].rename(columns={Indicator.HIGH.value: RuntimeIndicator.COMPARE.value})
                minute_close_df = ticker_minute_candle_df.loc[:, idx[:, Indicator.CLOSE.value]].rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
                minute_lower_body_df = ticker_minute_candle_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_BODY.value]].rename(columns={CustomisedIndicator.CANDLE_LOWER_BODY.value: RuntimeIndicator.COMPARE.value})
                
                minute_colour_df = ticker_minute_candle_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]].rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
                minute_volume_df = ticker_minute_candle_df.loc[:, idx[:, Indicator.VOLUME.value]].rename(columns={Indicator.VOLUME.value: RuntimeIndicator.COMPARE.value})
                
                gap_up_pct_df = (minute_lower_body_df.sub(ramp_up_close).div(ramp_up_close)).mul(100)
                gap_up_boolean_df = (gap_up_pct_df >= GAP_UP_PCT) & (minute_colour_df != CandleColour.GREY.value) & (minute_volume_df > 0)
                gap_up_occurrence_times_df = gap_up_boolean_df.cumsum().ffill().where(gap_up_boolean_df.values)
                
                new_high_test_lower_limit = (1 - (TEST_NEW_HIGH_TOLERANCE / 100)) * ramp_up_high
                new_high_test_upper_limit = (1 + (TEST_NEW_HIGH_TOLERANCE / 100)) * ramp_up_high
                
                minute_close_new_high_test_boolean_df = (minute_close_df >= new_high_test_lower_limit) & (minute_close_df <= new_high_test_upper_limit) & (minute_colour_df != CandleColour.GREY.value) & (minute_volume_df > 0)
                minute_high_new_high_test_boolean_df = (minute_high_df >= new_high_test_lower_limit) & (minute_high_df <= new_high_test_upper_limit) & (minute_colour_df != CandleColour.GREY.value) & (minute_volume_df > 0)
                
                continuation_boolean_df = (gap_up_boolean_df) | (minute_close_new_high_test_boolean_df | minute_high_new_high_test_boolean_df)
                
                ticker_to_occurrence_idx_list_dict = get_ticker_to_occurrence_idx_list(continuation_boolean_df)
                occurrence_idx_list = ticker_to_occurrence_idx_list_dict[ticker]
                check_alert_datetime_list = []
                trigger_alert_datetime_list = []

                us_current_datetime = get_current_us_datetime()
                
                for dt in occurrence_idx_list:
                    if dt is None:
                        continue
                    else:
                        time_diff_in_minute = (us_current_datetime.replace(tzinfo=None) - dt).total_seconds() / 60
                        
                        if time_diff_in_minute <= MAX_CURRENT_DATETIME_AND_ALERT_DATETIME_DIFF_IN_MINUTE:
                            check_alert_datetime_list.append(dt)
                
                trigger_alert_datetime_display = f'Previous day top gainer continuation occurrence idx list: {str(occurrence_idx_list)}\nPrevious day top gainer continuation non none occurrence idx list: {str([dt for dt in occurrence_idx_list if dt is not None])}\nPrevious day top gainer continuation check alert datetime list: {check_alert_datetime_list}\n'
                
                if check_alert_datetime_list:
                    earilest_alert_datetime = min(check_alert_datetime_list)
                    
                    for occurrence_idx in check_alert_datetime_list:
                        if len(trigger_alert_datetime_list) > 0:
                            previous_alert_trigger_datetime = trigger_alert_datetime_list[-1]
                            time_diff_in_minute = math.floor(((occurrence_idx - previous_alert_trigger_datetime).total_seconds()) / 60)
                            
                            if (time_diff_in_minute >= MIN_ALERT_CHECKING_INTERVAL_IN_MINUTE 
                                    and len(trigger_alert_datetime_list) < MAX_NO_OF_ALERT_TIMES):
                                trigger_alert_datetime_list.append(occurrence_idx)
                        else:
                            trigger_alert_datetime_list.append(earilest_alert_datetime)
                
                for pos, trigger_alert_datetime in enumerate(trigger_alert_datetime_list):    
                    check_message_sent_start_time = time.time()
                    
                    candlestick_display_start_datetime = None
                    if pos > 0:
                        candlestick_display_start_datetime = trigger_alert_datetime_list[pos - 1]
                    else:
                        candlestick_display_start_datetime = self.__minute_df.index[0]
                    
                    candle_chart_data_df, daily_date_to_fake_minute_datetime_x_axis_dict = concat_daily_df_and_minute_df(daily_df=self.__daily_df, 
                                                                                                                         minute_df=self.__minute_df, 
                                                                                                                         hit_scanner_datetime=trigger_alert_datetime, 
                                                                                                                         select_datetime_start_range=candlestick_display_start_datetime,
                                                                                                                         gap_btw_daily_and_minute=DAILY_AND_MINUTE_CANDLE_GAP)
                    
                    is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, hit_scanner_datetime=trigger_alert_datetime.replace(second=0, microsecond=0), pattern=PATTERN_NAME, bar_size=BarSize.ONE_MINUTE)
                    logger.log_debug_msg(f'Check {ticker} previous day continuation pattern message send time: {time.time() - check_message_sent_start_time} seconds')
                    
                    if not is_message_sent:
                        # Add alert trigger datetime log
                        trigger_alert_datetime_display += f'{ticker} previous day continuation alert trigger, datetime:'
                        for index, dt in enumerate(trigger_alert_datetime_list):
                            trigger_alert_datetime_display += dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            if index != len(trigger_alert_datetime_list) - 1:
                                trigger_alert_datetime_display += '\n'
                                
                        if SHOW_DISCORD_DEBUG_LOG:
                            self._discord_client.send_message(DiscordMessage(content=trigger_alert_datetime_display), DiscordChannel.PREVIOUS_DAYS_TOP_GAINER_CONTINUATION_ALERT_LOG)
                
                        contract_info = self.__ticker_to_contract_info_dict[ticker]
                        
                        one_minute_chart_start_time = time.time()
                        logger.log_debug_msg(f'Generate {ticker} previous day top gainer continuation one minute chart')
                        chart_dir = get_candlestick_chart(candle_data_df=candle_chart_data_df,
                                                          ticker=ticker, pattern=PATTERN_NAME, bar_size=BarSize.ONE_MINUTE,
                                                          daily_date_to_fake_minute_datetime_x_axis_dict=daily_date_to_fake_minute_datetime_x_axis_dict,
                                                          hit_scanner_datetime=trigger_alert_datetime,
                                                          positive_offset=0, negative_offset=0,
                                                          scatter_symbol=ScatterSymbol.NEW_HIGH_TEST, scatter_colour=ScatterColour.GREEN)
                        logger.log_debug_msg(f'Generate {ticker} previous day top gainer continuation one minute chart finished time: {time.time() - one_minute_chart_start_time} seconds')

                        hit_scanner_datetime_display = convert_into_human_readable_time(trigger_alert_datetime)
                        read_out_new_high_test_time = convert_into_read_out_time(trigger_alert_datetime)
                        
                        lower_body = ticker_minute_candle_df.loc[trigger_alert_datetime, (ticker, CustomisedIndicator.CANDLE_LOWER_BODY.value)]
                        close = ticker_minute_candle_df.loc[trigger_alert_datetime, (ticker, Indicator.CLOSE.value)]
                        high = ticker_minute_candle_df.loc[trigger_alert_datetime, (ticker, Indicator.HIGH.value)]
                        volume = ticker_minute_candle_df.loc[trigger_alert_datetime, (ticker, Indicator.VOLUME.value)]
                        total_volume = ticker_minute_candle_df.loc[trigger_alert_datetime, (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]
                        
                        gap_up_pct = round(((lower_body - ramp_up_close) / ramp_up_close) * 100, 2)
                        gap_up_occurrence_times = gap_up_occurrence_times_df.loc[trigger_alert_datetime, (ticker, RuntimeIndicator.COMPARE.value)]
                        
                        max_high_date = daily_high_df.idxmax()[(ticker, Indicator.HIGH.value)]
                        
                        #Continuation Message Precedence, New High Test > Gap Up
                        if gap_up_occurrence_times:
                            if (gap_up_pct >= GAP_UP_PCT 
                                    and gap_up_occurrence_times <= TOP_N_GAP_UP_OCCURRENCE_TIMES
                                    and ramp_up_candle_date == max_high_date):
                                close_pct = round(((close - yesterday_close) / yesterday_close) * 100, 2)
                                display_msg = f'{ticker} is gapping up {gap_up_pct}% to test previous days\'s high of {ramp_up_high} at {hit_scanner_datetime_display}, current close: {close}, close change: {close_pct}%'
                                readout_msg = f'{" ".join(ticker)} is gapping up {gap_up_pct}% to test previous day\'s high of {ramp_up_high} at {read_out_new_high_test_time}'
                            else:
                                continue
                        
                        if (high >= new_high_test_lower_limit or high <= new_high_test_upper_limit):
                            if ramp_up_candle_date == max_high_date:
                                high_to_previous_close_pct = round(((high - yesterday_close) / yesterday_close) * 100, 2)
                                display_msg = f'{ticker} is testing previous days\'s high of {ramp_up_high} at {hit_scanner_datetime_display}, current high: {high}, high to previous close change: {high_to_previous_close_pct}%'
                                readout_msg = f'{" ".join(ticker)} is testing previous day\'s high of {ramp_up_high} at {read_out_new_high_test_time}'
                            else:
                                logger.log_debug_msg(f'Skip top gainer {ticker} continuation analysis, ramp up candle date: {ramp_up_candle_date}, maximum high date: {max_high_date}')
                                logger.log_df_debug_msg(self.__daily_df)
                                continue
                            
                        message = ScannerResultMessage(title=display_msg,
                                                       readout_msg=readout_msg,
                                                       close=close,
                                                       yesterday_close=yesterday_close,
                                                       volume=volume, total_volume=total_volume,
                                                       contract_info=contract_info,
                                                       chart_dir=chart_dir,
                                                       ticker=ticker,
                                                       hit_scanner_datetime=trigger_alert_datetime.replace(second=0, microsecond=0),
                                                       pattern=PATTERN_NAME,
                                                       bar_size=BarSize.ONE_MINUTE)
                        message_list.append(message)

        if message_list:
            send_msg_start_time = time.time()
            self.send_notification(message_list, DiscordChannel.PREVIOUS_DAYS_TOP_GAINER_CONTINUATION, False)
            logger.log_debug_msg(f'{PATTERN_NAME} send message time: {time.time() - send_msg_start_time} seconds')