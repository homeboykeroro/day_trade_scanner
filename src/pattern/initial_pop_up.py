import time
import pandas as pd
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from model.discord_scanner_message import DiscordScannerMessage

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.dataframe_util import derive_idx_df
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time

from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class InitialPopUp(PatternAnalyser):
    MIN_GAP_UP_PCT = 4
    MIN_PREVIOUS_CLOSE_PCT = 15
        
    def __init__(self, historical_data_df: DataFrame, ticker_to_contract_dict: dict):
        self.__historical_data_df = historical_data_df
        self.__ticker_to_contract_dict = ticker_to_contract_dict

    def analyse(self) -> list:
        message_list = []
        logger.log_debug_msg('Initial pop up scan')
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
        first_occurrence_idx_np = numeric_idx_df.where(pop_up_boolean_df.values).idxmin().values
        first_pop_up_occurrence_df = (numeric_idx_df == first_occurrence_idx_np)
        
        datetime_idx_df = derive_idx_df(first_pop_up_occurrence_df, numeric_idx=False).rename(columns={RuntimeIndicator.COMPARE.value: RuntimeIndicator.INDEX.value})
        pop_up_datetime_idx_df = datetime_idx_df.where(first_pop_up_occurrence_df.values).ffill()
        result_boolean_df = pop_up_datetime_idx_df.notna()
        
        new_gainer_result_series = result_boolean_df.any()   
        new_gainer_ticker_list = new_gainer_result_series.index[new_gainer_result_series].get_level_values(0).tolist()

        logger.log_debug_msg(f'Initial pop up analysis time: {time.time() - start_time} seconds')

        if len(new_gainer_ticker_list) > 0:
            for ticker in new_gainer_ticker_list:
                contract_info = self.__ticker_to_contract_dict[ticker]
                
                pop_up_close_df = (self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
                                                            .where(first_pop_up_occurrence_df.values)
                                                            .ffill())
                pop_up_previous_close_df = (previous_close_df.where(first_pop_up_occurrence_df.values)
                                                             .ffill())
                pop_up_pct_change_df = (self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]]
                                                                 .where(first_pop_up_occurrence_df.values)
                                                                 .ffill())
                pop_up_previous_close_pct_df = (previous_close_pct_df.where(first_pop_up_occurrence_df.values)
                                                                     .ffill())
                pop_up_volume_df = (self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME.value]]
                                                             .where(first_pop_up_occurrence_df.values)
                                                             .ffill())
                pop_up_total_volume_df = (self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.TOTAL_VOLUME.value]]
                                                                   .where(first_pop_up_occurrence_df.values)
                                                                   .ffill())
                pop_up_datetime_idx_df = (datetime_idx_df.where(first_pop_up_occurrence_df.values)
                                                         .ffill())
                
                close = pop_up_close_df.loc[pop_up_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                previous_close = pop_up_previous_close_df.loc[pop_up_previous_close_df.index[-1], (ticker, CustomisedIndicator.PREVIOUS_CLOSE.value)]
                close_pct = pop_up_pct_change_df.loc[pop_up_pct_change_df.index[-1], (ticker, CustomisedIndicator.CLOSE_CHANGE.value)]
                previous_close_pct = round(pop_up_previous_close_pct_df.loc[pop_up_previous_close_pct_df.index[-1], (ticker, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE.value)], 2)
                volume = "{:,}".format(int(pop_up_volume_df.loc[pop_up_volume_df.index[-1], (ticker, Indicator.VOLUME.value)]))
                total_volume = "{:,}".format(int(pop_up_total_volume_df.loc[pop_up_total_volume_df.index[-1], (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]))
                pop_up_time = pop_up_datetime_idx_df.loc[pop_up_datetime_idx_df.index[-1], (ticker, RuntimeIndicator.INDEX.value)]
                pop_up_time_display = convert_into_human_readable_time(pop_up_time)
                read_out_pop_up_time = convert_into_read_out_time(pop_up_time)
                
                message_title = f'**{ticker}** is popping up {previous_close_pct}% at {pop_up_time_display}\n' 
                price_display = f'**Close: ${close}   Previous Close: ${previous_close}**\n'
                pop_up_volume_display = f'**Volume: {volume}   Total Volume: ${total_volume}**\n'
                display_message = message_title + price_display + pop_up_volume_display + str(contract_info)
                
                read_out_ticker = " ".join(ticker)
                read_out_message = f'{read_out_ticker} is popping up {previous_close_pct} percent at {read_out_pop_up_time}'
                
                message = DiscordScannerMessage(display_message, read_out_message)
                message_list.append(message)
   
        return message_list