import time
import pandas as pd
import discord
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from model.discord.discord_scanner_message import DiscordScannerMessage

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour
from constant.candle.bar_size import BarSize

from utils.chat_util import get_candlestick_chart
from utils.dataframe_util import derive_idx_df
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class InitialPop(PatternAnalyser):
    MIN_GAP_UP_PCT = 5
    MIN_YESTERDAY_CLOSE_TO_LAST_PCT = 15
        
    def __init__(self, bar_size: BarSize, historical_data_df: DataFrame, yesterday_daily_candle_df: DataFrame, ticker_to_contract_info_dict: dict, discord_client, sqlite_connector):
        super().__init__(discord_client, sqlite_connector)
        self.__bar_size = bar_size
        self.__historical_data_df = historical_data_df
        self.__yesterday_daily_candle_df = yesterday_daily_candle_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Initial pop scan')
        start_time = time.time()
        
        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
        
        yesterday_close_df = self.__yesterday_daily_candle_df.loc[:, idx[:, Indicator.CLOSE.value]]
        yesterday_close_to_last_pct_df = (close_df.sub(yesterday_close_df.values)
                                                  .div(yesterday_close_df.values)
                                                  .mul(100))
        
        yesterday_upper_body_df = self.__yesterday_daily_candle_df.loc[:, idx[:, CustomisedIndicator.CANDLE_UPPER_BODY.value]]
        lower_body_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_BODY.value]]
        gap_up_pct_df = (lower_body_df.sub(yesterday_upper_body_df.values)
                                      .div(yesterday_upper_body_df.values)
                                      .mul(100))
        
        min_gap_up_pct_df = (gap_up_pct_df >= self.MIN_GAP_UP_PCT).rename(columns={CustomisedIndicator.CANDLE_LOWER_BODY.value: RuntimeIndicator.COMPARE.value})
        min_yesterday_close_to_last_pct_boolean_df = (yesterday_close_to_last_pct_df >= self.MIN_YESTERDAY_CLOSE_TO_LAST_PCT).rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
        non_flat_candle_boolean_df = (candle_colour_df != CandleColour.GREY.value).rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        pop_up_boolean_df = (min_gap_up_pct_df) & (min_yesterday_close_to_last_pct_boolean_df) & (non_flat_candle_boolean_df)
        numeric_idx_df = derive_idx_df(pop_up_boolean_df)
        first_occurrence_idx_np = numeric_idx_df.where(pop_up_boolean_df.values).idxmin().values
        first_pop_up_occurrence_df = (numeric_idx_df == first_occurrence_idx_np)
        
        datetime_idx_df = derive_idx_df(pop_up_boolean_df, numeric_idx=False)
        pop_up_datetime_idx_df = datetime_idx_df.where(first_pop_up_occurrence_df.values).ffill()
        result_boolean_df = pop_up_datetime_idx_df.notna()
        
        new_gainer_result_series = result_boolean_df.any()   
        new_gainer_ticker_list = new_gainer_result_series.index[new_gainer_result_series].get_level_values(0).tolist()
        
        logger.log_debug_msg(f'Initial pop analysis time: {time.time() - start_time} seconds')

        with pd.option_context('display.max_rows', None,
                   'display.max_columns', None,
                   'display.precision', 3,
                   ):
            logger.log_debug_msg(f'self.__historical_data_df: {self.__historical_data_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'yesterday_close_to_last_pct_df: {yesterday_close_to_last_pct_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'candle_colour_df: {candle_colour_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'gap_up_pct_df: {gap_up_pct_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'yesterday_close_df: {yesterday_close_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'pop_up_boolean_df: {pop_up_boolean_df}', with_log_file=True, with_std_out=False)
            
        if len(new_gainer_ticker_list) > 0:
            send_msg_start_time = time.time()
            
            for ticker in new_gainer_ticker_list:
                contract_info = self.__ticker_to_contract_info_dict[ticker]
                
                pop_up_close_df = (self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
                                                            .where(first_pop_up_occurrence_df.values)
                                                            .ffill())
                pop_up_yesterday_close_df = yesterday_close_df.loc[:, idx[[ticker], Indicator.CLOSE.value]]
                pop_up_yesterday_close_to_last_pct_df = (yesterday_close_to_last_pct_df.where(first_pop_up_occurrence_df.values)
                                                                              .ffill())
                pop_up_volume_df = (self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME.value]]
                                                             .where(first_pop_up_occurrence_df.values)
                                                             .ffill())
                pop_up_total_volume_df = (self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.TOTAL_VOLUME.value]]
                                                                   .where(first_pop_up_occurrence_df.values)
                                                                   .ffill())
                
                close = pop_up_close_df.loc[pop_up_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                yesterday_close = pop_up_yesterday_close_df.loc[pop_up_yesterday_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                yesterday_close_to_last_pct = pop_up_yesterday_close_to_last_pct_df.loc[pop_up_yesterday_close_to_last_pct_df.index[-1], (ticker, Indicator.CLOSE.value)]
                volume = "{:,}".format(int(pop_up_volume_df.loc[pop_up_volume_df.index[-1], (ticker, Indicator.VOLUME.value)]))
                total_volume = "{:,}".format(int(pop_up_total_volume_df.loc[pop_up_total_volume_df.index[-1], (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]))
                pop_up_time = pop_up_datetime_idx_df.loc[pop_up_datetime_idx_df.index[-1], (ticker, RuntimeIndicator.INDEX.value)]
                pop_up_time_display = convert_into_human_readable_time(pop_up_time)
                read_out_pop_up_time = convert_into_read_out_time(pop_up_time)
                
                embed = discord.Embed(title=f'{ticker} is popping up {round(yesterday_close_to_last_pct, 2)}% at {pop_up_time_display}')
                embed.add_field(name = 'Close:', value= f'${close}', inline = True)
                embed.add_field(name = 'Previous Close:', value = f'${yesterday_close}', inline = True)
                embed.add_field(name = chr(173), value = chr(173))
                embed.add_field(name = 'Volume:', value = f'${volume}', inline = True)
                embed.add_field(name = 'Total Volume:', value = f'${total_volume}', inline = True)
                embed.add_field(name = chr(173), value = chr(173))
                contract_info.add_contract_info_to_embed_msg(embed)
                
                read_out_ticker = " ".join(ticker)
                read_out_message = f'{read_out_ticker} is popping up {round(yesterday_close_to_last_pct, 2)} percent at {read_out_pop_up_time}'
                
                chart_dir = get_candlestick_chart(pattern='INITIAL_POP', 
                                                  bar_size=self.__bar_size,
                                                  sub_panel_df=None,
                                                  candle_start_range=pop_up_time - pd.Timedelta(minutes=30),
                                                  candle_end_range=pop_up_time - pd.Timedelta(minutes=30))
                message = DiscordScannerMessage(embed=embed, 
                                                display_message=None, read_out_message=read_out_message, 
                                                ticker=ticker, 
                                                hit_scanner_datetime=pop_up_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                pattern='INITIAL_POP',
                                                bar_size=self.__bar_size.value)
                message_list.append(message)
                
            logger.log_debug_msg(f'Initial pop send message time: {time.time() - send_msg_start_time} seconds')
         
        if message_list:       
            self.send_notification(message_list)
    