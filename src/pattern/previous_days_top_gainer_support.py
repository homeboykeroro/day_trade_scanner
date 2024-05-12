import math
import time
import pandas as pd

from model.discord.scanner_result_message import ScannerResultMessage

from pattern.pattern_analyser import PatternAnalyser

from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time, get_current_us_datetime
from utils.dataframe_util import get_ticker_to_occurrence_idx_list, replace_daily_df_latest_day_with_minute
from utils.chart_util import get_candlestick_chart
from utils.config_util import get_config
from utils.logger import Logger

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

PATTERN_NAME = 'PREVIOUS_DAY_TOP_GAINER_SUPPORT'

MIN_MULTI_DAYS_CLOSE_CHANGE_PCT = get_config('MULTI_DAYS_TOP_GAINER_SCAN_PARAM', 'MIN_MULTI_DAYS_CLOSE_CHANGE_PCT')
MAX_TOLERANCE_PERIOD_IN_MINUTE = get_config('PREVIOUS_DAY_TOP_GAINER_SUPPORT_PARAM', 'MAX_TOLERANCE_PERIOD_IN_MINUTE')
RANGE_TOLERANCE = get_config('PREVIOUS_DAY_TOP_GAINER_SUPPORT_PARAM', 'RANGE_TOLERANCE')
MINUTE_CANDLE_POSITIVE_OFFSET = get_config('PREVIOUS_DAY_TOP_GAINER_SUPPORT_PARAM', 'MINUTE_CANDLE_POSITIVE_OFFSET')
MINUTE_CANDLE_NEGATIVE_OFFSET = get_config('PREVIOUS_DAY_TOP_GAINER_SUPPORT_PARAM', 'MINUTE_CANDLE_NEGATIVE_OFFSET')

LOWER_RANGE_TOLERANCE_FACTOR = 1 - (RANGE_TOLERANCE / 100)
UPPER_RANGE_TOLERANCE_FACTOR = 1 + (RANGE_TOLERANCE / 100)

class PreviousDayTopGainerSupport(PatternAnalyser): 
    def __init__(self, daily_df: pd.DataFrame, minute_df: pd.DataFrame, ticker_to_contract_info_dict: dict, discord_client):
        super().__init__(discord_client)
        self.__daily_df = daily_df
        self.__minute_df = minute_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict
    
    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Previous day top gainers support scan')

        daily_close_pct_df = self.__daily_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]].apply(pd.to_numeric, errors='coerce')
        daily_volume_df = self.__daily_df.loc[:, idx[:, Indicator.VOLUME.value]].apply(pd.to_numeric, errors='coerce')
        daily_candle_colour_df = self.__daily_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
        
        max_daily_close_pct_series = (daily_close_pct_df.max() >= MIN_MULTI_DAYS_CLOSE_CHANGE_PCT).rename(index={CustomisedIndicator.CLOSE_CHANGE.value : RuntimeIndicator.COMPARE.value})
        max_daily_volume_dt_index_series = daily_volume_df.idxmax().rename(index={Indicator.VOLUME.value: RuntimeIndicator.COMPARE.value})
        max_daily_close_pct_dt_index_series = daily_close_pct_df.idxmax().rename(index={CustomisedIndicator.CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
        max_daily_close_with_most_volume_ticker_series = (max_daily_volume_dt_index_series == max_daily_close_pct_dt_index_series) & (max_daily_close_pct_series)
        previous_day_top_gainer_ticker_list = max_daily_close_with_most_volume_ticker_series.index[max_daily_close_with_most_volume_ticker_series].get_level_values(0).tolist()

        latest_daily_candle_date = self.__daily_df.index[-1]
        
        for ticker in previous_day_top_gainer_ticker_list:
            ramp_up_candle_date = max_daily_volume_dt_index_series[(ticker, RuntimeIndicator.COMPARE.value)]
            candle_colour = daily_candle_colour_df.loc[ramp_up_candle_date, (ticker, CustomisedIndicator.CANDLE_COLOUR.value)]
            
            is_green = candle_colour == CandleColour.GREEN.value
            ticker_minute_candle_df = self.__minute_df.loc[:, idx[ticker, :]]
            
            if is_green:
                ramp_up_open = self.__daily_df.loc[ramp_up_candle_date, (ticker, Indicator.OPEN.value)]
                ramp_up_low = self.__daily_df.loc[ramp_up_candle_date, (ticker, Indicator.LOW.value)]
                
                support_low_lower_limit = ramp_up_low * LOWER_RANGE_TOLERANCE_FACTOR
                support_low_upper_limit = ramp_up_low * UPPER_RANGE_TOLERANCE_FACTOR
                support_open_lower_limit = ramp_up_open * LOWER_RANGE_TOLERANCE_FACTOR
                support_open_upper_limit = ramp_up_open * UPPER_RANGE_TOLERANCE_FACTOR
                
                minute_low_df = ticker_minute_candle_df.loc[:, idx[:, Indicator.LOW.value]].rename(columns={Indicator.LOW.value: RuntimeIndicator.COMPARE.value})
                minute_close_df = ticker_minute_candle_df.loc[:, idx[:, Indicator.CLOSE.value]].rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
                
                #min_low = minute_low_df.min()[(ticker, Indicator.LOW.value)]
                
                low_hit_support_low_boolean_df = (minute_low_df >= support_low_lower_limit) & (minute_low_df <= support_low_upper_limit)
                close_hit_support_low_boolean_df = (minute_close_df >= support_low_lower_limit) & (minute_close_df <= support_low_upper_limit)
                low_hit_support_open_boolean_df = (minute_low_df >= support_open_lower_limit) & (minute_low_df <= support_open_upper_limit)
                close_hit_support_open_boolean_df = (minute_close_df >= support_open_lower_limit) & (minute_close_df <= support_open_upper_limit)
                
                hit_support_boolean_df = low_hit_support_low_boolean_df | close_hit_support_low_boolean_df | low_hit_support_open_boolean_df | close_hit_support_open_boolean_df
                
                ticker_to_occurrence_idx_list_dict = get_ticker_to_occurrence_idx_list(hit_support_boolean_df)
                occurrence_idx_list = ticker_to_occurrence_idx_list_dict[ticker]

                for occurrence_idx in occurrence_idx_list:   
                    if not occurrence_idx:
                        continue
                    else:
                        us_current_datetime = get_current_us_datetime()
                        current_datetime_and_pop_up_time_diff = math.floor(((us_current_datetime.replace(tzinfo=None) - occurrence_idx).total_seconds()) / 60)
                        
                        if current_datetime_and_pop_up_time_diff > MAX_TOLERANCE_PERIOD_IN_MINUTE:
                            logger.log_debug_msg(f'Exclude {ticker} previous day support at {occurrence_idx}, analysis datetime: {us_current_datetime}, out of tolerance period')
                            continue
                    
                    hit_support_time = occurrence_idx
                    check_message_sent_start_time = time.time()
                    is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, hit_scanner_datetime=hit_support_time.replace(second=0, microsecond=0), pattern=PATTERN_NAME, bar_size=BarSize.ONE_MINUTE)
                    logger.log_debug_msg(f'Check {ticker} previous day support pattern message send time: {time.time() - check_message_sent_start_time} seconds')
                    
                    if not is_message_sent:
                        contract_info = self.__ticker_to_contract_info_dict[ticker]
                        
                        daily_df = replace_daily_df_latest_day_with_minute(daily_df=self.__daily_df.loc[:, idx[[ticker], :]], 
                                                                           minute_df=self.__minute_df.loc[[hit_support_time], idx[[ticker], :]])
                        
                        one_minute_chart_start_time = time.time()
                        logger.log_debug_msg(f'Generate {ticker} previous day top gainer support one minute chart')
                        minute_chart_dir = get_candlestick_chart(candle_data_df=self.__minute_df,
                                                                 ticker=ticker, pattern=PATTERN_NAME, bar_size=BarSize.ONE_MINUTE,
                                                                 hit_scanner_datetime=hit_support_time,
                                                                 positive_offset=MINUTE_CANDLE_POSITIVE_OFFSET, negative_offset=MINUTE_CANDLE_NEGATIVE_OFFSET,
                                                                 scatter_symbol=ScatterSymbol.SUPPORT, scatter_colour=ScatterColour.RED)
                        logger.log_debug_msg(f'Generate {ticker} previous day top gainer support one minute chart finished time: {time.time() - one_minute_chart_start_time} seconds')
                        daily_chart_start_time = time.time()
                        logger.log_debug_msg(f'Generate {ticker} previous day top gainer support daily chart')
                        daily_chart_dir = get_candlestick_chart(candle_data_df=daily_df,
                                                                ticker=ticker, pattern=PATTERN_NAME, bar_size=BarSize.ONE_DAY,
                                                                hit_scanner_datetime=ramp_up_candle_date,
                                                                scatter_symbol=ScatterSymbol.SUPPORT, scatter_colour=ScatterColour.RED)
                        logger.log_debug_msg(f'Generate {ticker} previous top gainer support finished time: {time.time() - daily_chart_start_time} seconds')
                        
                    hit_scanner_datetime_display = convert_into_human_readable_time(hit_support_time)
                    read_out_hit_support_time = convert_into_read_out_time(hit_support_time)
                    
                    yesterday_close = self.__daily_df.loc[latest_daily_candle_date, (ticker, Indicator.CLOSE.value)]
                    low = ticker_minute_candle_df.loc[hit_support_time, (ticker, Indicator.LOW.value)]
                    close = ticker_minute_candle_df.loc[hit_support_time, (ticker, Indicator.CLOSE.value)]
                    volume = ticker_minute_candle_df.loc[hit_support_time, (ticker, Indicator.VOLUME.value)]
                    total_volume = ticker_minute_candle_df.loc[hit_support_time, (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]
                    
                    if (low >= support_low_lower_limit and low <= support_low_upper_limit):
                        indicator = 'low'
                        support = ramp_up_low
                        last_pct_change = round(((low - yesterday_close)/ yesterday_close) * 100, 2)
                        
                    if (low >= support_open_lower_limit and low <= support_open_upper_limit):
                        indicator = 'low'
                        support = ramp_up_open
                        last_pct_change = round(((low - yesterday_close)/ yesterday_close) * 100, 2)
                    
                    if (close >= support_low_lower_limit and close <= support_low_upper_limit):
                        indicator = 'close'
                        support = ramp_up_low
                        last_pct_change = round(((close - yesterday_close)/ yesterday_close) * 100, 2)
                        
                    if (close >= support_open_lower_limit and close <= support_open_upper_limit):
                        indicator = 'close'
                        support = ramp_up_open
                        last_pct_change = round(((close - yesterday_close)/ yesterday_close) * 100, 2)
                    
                    message = ScannerResultMessage(title=f'{ticker}\'s {indicator} is hitting previous day support of {support} ({last_pct_change}%) at {hit_scanner_datetime_display}',
                                                   readout_msg=f'{" ".join(ticker)} is hitting previous day support of {support} at {read_out_hit_support_time}',
                                                   close=close,
                                                   yesterday_close=yesterday_close,
                                                   volume=volume, total_volume=total_volume,
                                                   contract_info=contract_info,
                                                   minute_chart_dir=minute_chart_dir,
                                                   daily_chart_dir=daily_chart_dir, 
                                                   ticker=ticker,
                                                   hit_scanner_datetime=hit_support_time.replace(second=0, microsecond=0),
                                                   pattern=PATTERN_NAME,
                                                   bar_size=BarSize.ONE_MINUTE)
                    message_list.append(message)

        if message_list:
            send_msg_start_time = time.time()
            self.send_notification(message_list, DiscordChannel.PREVIOUS_TOP_GAINER_SUPPORT)
            logger.log_debug_msg(f'{PATTERN_NAME} send message time: {time.time() - send_msg_start_time} seconds')