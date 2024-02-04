import os
import time
import numpy as np
import pandas as pd
import discord
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from model.discord.discord_scanner_message import DiscordScannerMessage

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.scatter_colour import ScatterColour
from constant.candle.candle_colour import CandleColour
from constant.candle.bar_size import BarSize

from utils.chart_util import get_candlestick_chart
from utils.dataframe_util import replace_daily_df_latest_day_with_minute, get_ticker_to_occurrence_idx_list
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time, get_offsetted_hit_scanner_datetime
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class InitialPop(PatternAnalyser):
    MAX_POP_OCCURRENCE = 3
    MIN_GAP_UP_PCT = 5
    MIN_YESTERDAY_CLOSE_TO_LAST_PCT = 15
        
    def __init__(self, bar_size: BarSize, historical_data_df: DataFrame, daily_df: DataFrame, ticker_to_contract_info_dict: dict, discord_client, sqlite_connector):
        super().__init__(discord_client, sqlite_connector)
        self.__bar_size = bar_size
        self.__historical_data_df = historical_data_df
        self.__daily_df = daily_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Initial pop scan')
        start_time = time.time()
        
        yesterday_daily_candle_df = self.__daily_df.iloc[[-2]]
        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
        
        yesterday_close_df = yesterday_daily_candle_df.loc[:, idx[:, Indicator.CLOSE.value]]
        yesterday_close_to_last_pct_df = (close_df.sub(yesterday_close_df.values)
                                                  .div(yesterday_close_df.values)
                                                  .mul(100))
        
        yesterday_upper_body_df = yesterday_daily_candle_df.loc[:, idx[:, CustomisedIndicator.CANDLE_UPPER_BODY.value]]
        candle_lower_body_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_BODY.value]]
        gap_up_pct_df = (candle_lower_body_df.sub(yesterday_upper_body_df.values)
                                      .div(yesterday_upper_body_df.values)
                                      .mul(100))
        
        self.__historical_data_df.loc[[self.__historical_data_df.index[0]], idx[:, CustomisedIndicator.CLOSE_CHANGE.value]] = yesterday_close_to_last_pct_df.iloc[[0]]
        self.__historical_data_df.loc[[self.__historical_data_df.index[0]], idx[:, CustomisedIndicator.GAP_PCT_CHANGE.value]] = gap_up_pct_df.iloc[[0]]
        
        min_gap_up_pct_df = (gap_up_pct_df >= self.MIN_GAP_UP_PCT).rename(columns={CustomisedIndicator.CANDLE_LOWER_BODY.value: RuntimeIndicator.COMPARE.value})
        min_yesterday_close_to_last_pct_boolean_df = (yesterday_close_to_last_pct_df >= self.MIN_YESTERDAY_CLOSE_TO_LAST_PCT).rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
        non_flat_candle_boolean_df = (candle_colour_df != CandleColour.GREY.value).rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        pop_up_boolean_df = (min_gap_up_pct_df) & (min_yesterday_close_to_last_pct_boolean_df) & (non_flat_candle_boolean_df)
        
        top_gainer_result_series = pop_up_boolean_df.any()   
        top_gainer_ticker_list = top_gainer_result_series.index[top_gainer_result_series].get_level_values(0).tolist()
        
        ticker_to_occurrence_idx_list_dict = get_ticker_to_occurrence_idx_list(pop_up_boolean_df, self.MAX_POP_OCCURRENCE)
        logger.log_debug_msg(f'Initial pop ticker to occurrence idx list: {ticker_to_occurrence_idx_list_dict}')
        logger.log_debug_msg(f'Initial pop analysis time: {time.time() - start_time} seconds')
            
        if len(top_gainer_ticker_list) > 0:
            for ticker in top_gainer_ticker_list:
                occurrence_idx_list = ticker_to_occurrence_idx_list_dict[ticker]

                for occurrence_idx in occurrence_idx_list:   
                    if not occurrence_idx:
                        continue
                    
                    pop_up_time = occurrence_idx
                    message = DiscordScannerMessage(ticker=ticker, 
                                                    hit_scanner_datetime=pop_up_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                    pattern='INITIAL_POP',
                                                    bar_size=self.__bar_size.value)
                
                    is_message_sent = self.check_if_message_sent(message)
                
                    if not is_message_sent:
                        logger.log_debug_msg(f'{ticker} Dataframe: {self.__historical_data_df.loc[:, idx[[ticker], :]]}')
                        contract_info = self.__ticker_to_contract_info_dict[ticker]

                        close = self.__historical_data_df.loc[pop_up_time, (ticker, Indicator.CLOSE.value)]
                        volume = "{:,}".format(self.__historical_data_df.loc[pop_up_time, (ticker, Indicator.VOLUME.value)])
                        total_volume = "{:,}".format(self.__historical_data_df.loc[pop_up_time, (ticker, CustomisedIndicator.TOTAL_VOLUME.value)])
                        yesterday_close = yesterday_close_df.loc[yesterday_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                        yesterday_close_to_last_pct = yesterday_close_to_last_pct_df.loc[pop_up_time, (ticker, Indicator.CLOSE.value)]

                        pop_up_time_display = convert_into_human_readable_time(pop_up_time)
                        read_out_pop_up_time = convert_into_read_out_time(pop_up_time)

                        daily_df = replace_daily_df_latest_day_with_minute(daily_df=self.__daily_df.loc[:, idx[[ticker], :]], 
                                                                           minute_df=self.__historical_data_df.loc[[pop_up_time], idx[[ticker], :]])
                        
                        minute_chart_dir = get_candlestick_chart(candle_data_df=self.__historical_data_df,
                                                                 ticker=ticker, pattern='INITIAL_POP', bar_size=self.__bar_size,
                                                                 hit_scanner_datetime=pop_up_time,
                                                                 positive_offset=3, negative_offset=2,
                                                                 scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.BLUE)
                        daily_chart_dir = get_candlestick_chart(candle_data_df=daily_df,
                                                                ticker=ticker, pattern='INITIAL_POP', bar_size=BarSize.ONE_DAY,
                                                                hit_scanner_datetime=daily_df.index[-1],
                                                                scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.BLUE)
                        
                        embed = discord.Embed(title=f'{ticker} is popping up {round(yesterday_close_to_last_pct, 2)}% at {pop_up_time_display}')
                        embed.add_field(name = 'Close:', value= f'${close}', inline = True)
                        embed.add_field(name = 'Previous Close:', value = f'${yesterday_close}', inline = True)
                        embed.add_field(name = chr(173), value = chr(173))
                        embed.add_field(name = 'Volume:', value = f'{volume}', inline = True)
                        embed.add_field(name = 'Total Volume:', value = f'{total_volume}', inline = True)
                        embed.add_field(name = chr(173), value = chr(173))
                        embed.set_image(url=f"attachment://{os.path.basename(minute_chart_dir)}")
                        embed.set_image(url=f"attachment://{os.path.basename(daily_chart_dir)}")
                        contract_info.add_contract_info_to_embed_msg(embed)

                        message.embed = embed
                        message.candle_chart_list = [discord.File(minute_chart_dir, filename=os.path.basename(minute_chart_dir)),
                                                     discord.File(daily_chart_dir, filename=os.path.basename(daily_chart_dir))]
                        message.read_out_message = f'{" ".join(ticker)} is popping up {round(yesterday_close_to_last_pct, 2)} percent at {read_out_pop_up_time}'
                        message_list.append(message)
        
        if message_list:  
            send_msg_start_time = time.time()     
            self.send_notification(message_list)
            logger.log_debug_msg(f'Initial pop send message time: {time.time() - send_msg_start_time} seconds')
    