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
from utils.dataframe_util import derive_idx_df, get_candle_description_df, replace_daily_df_latest_day_with_minute
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time, get_offsetted_pd_datetime
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class InitialPop(PatternAnalyser):
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
            logger.log_debug_msg(f'self.__daily_df: {self.__daily_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'gap_up_pct_df: {gap_up_pct_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'yesterday_close_df: {yesterday_close_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'pop_up_boolean_df: {pop_up_boolean_df}', with_log_file=True, with_std_out=False)
            
        if len(new_gainer_ticker_list) > 0:
            for ticker in new_gainer_ticker_list:
                contract_info = self.__ticker_to_contract_info_dict[ticker]
                
                pop_up_close_df = (self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
                                                            .where(first_pop_up_occurrence_df.values)
                                                            .ffill())
                pop_up_yesterday_close_to_last_pct_df = (yesterday_close_to_last_pct_df.where(first_pop_up_occurrence_df.values)
                                                                                       .ffill())
                pop_up_volume_df = (self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME.value]]
                                                             .where(first_pop_up_occurrence_df.values)
                                                             .ffill())
                pop_up_total_volume_df = (self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.TOTAL_VOLUME.value]]
                                                                   .where(first_pop_up_occurrence_df.values)
                                                                   .ffill())
                
                pop_up_time = pop_up_datetime_idx_df.loc[pop_up_datetime_idx_df.index[-1], (ticker, RuntimeIndicator.INDEX.value)]
                                
                message = DiscordScannerMessage(ticker=ticker, 
                                                hit_scanner_datetime=pop_up_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                pattern='INITIAL_POP',
                                                bar_size=self.__bar_size.value)
                
                is_message_sent = self.check_if_message_sent(message)
                
                if not is_message_sent:
                    close = pop_up_close_df.loc[pop_up_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                    yesterday_close = yesterday_close_df.loc[yesterday_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                    yesterday_close_to_last_pct = pop_up_yesterday_close_to_last_pct_df.loc[pop_up_yesterday_close_to_last_pct_df.index[-1], (ticker, Indicator.CLOSE.value)]
                    volume = "{:,}".format(int(pop_up_volume_df.loc[pop_up_volume_df.index[-1], (ticker, Indicator.VOLUME.value)]))
                    total_volume = "{:,}".format(int(pop_up_total_volume_df.loc[pop_up_total_volume_df.index[-1], (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]))
                
                    pop_up_time_display = convert_into_human_readable_time(pop_up_time)
                    read_out_pop_up_time = convert_into_read_out_time(pop_up_time)
                  
                    minute_symbol_df = (pop_up_boolean_df.loc[:, idx[[ticker], :]]
                                                         .where(first_pop_up_occurrence_df.loc[:, idx[[ticker], :]].values)
                                                         .notnull()
                                                         .replace({True: ScatterSymbol.POP.value, False: 'none'}))
                    minute_colour_df = (pop_up_boolean_df.loc[:, idx[[ticker], :]]
                                                         .where(first_pop_up_occurrence_df.loc[:, idx[[ticker], :]].values)
                                                         .notnull()
                                                         .replace({True: ScatterColour.BLUE.value, False: 'none'}))
                    minute_description_df = get_candle_description_df(self.__historical_data_df.loc[:, idx[[ticker], :]])
                    candle_start_range, candle_end_range = get_offsetted_pd_datetime(pd_datetime=pop_up_time, 
                                                                                     negative_offset=2, 
                                                                                     positive_offset=3, 
                                                                                     latest_datetime=self.__historical_data_df.index[-1])
                  
                    minute_chart_dir = get_candlestick_chart(pattern='INITIAL_POP', 
                                                             main_df=self.__historical_data_df.loc[candle_start_range:candle_end_range, idx[[ticker], :]],
                                                             scatter_symbol_df=minute_symbol_df.loc[candle_start_range:candle_end_range, idx[[ticker], :]],
                                                             scatter_colour_df=minute_colour_df.loc[candle_start_range:candle_end_range, idx[[ticker], :]],
                                                             description_df=minute_description_df.loc[candle_start_range:candle_end_range, idx[[ticker], :]],
                                                             bar_size=self.__bar_size)
                    
                    daily_df = replace_daily_df_latest_day_with_minute(daily_df=self.__daily_df.loc[:, idx[[ticker], :]], 
                                                                       minute_df=self.__historical_data_df.loc[[pop_up_time], idx[[ticker], :]])
                    daily_symbol_df = daily_df.loc[:, idx[:, [Indicator.LOW.value]]].copy()
                    daily_symbol_df.loc[:, idx[:, [Indicator.LOW.value]]] = np.full((daily_df.shape[0], 1), 'none')
                    daily_colour_df = daily_df.loc[:, idx[:, [Indicator.LOW.value]]].copy()
                    daily_colour_df.loc[:, idx[:, [Indicator.LOW.value]]] = np.full((daily_df.shape[0], 1), 'none')
                    
                    daily_symbol_df.iat[-1, 0] = ScatterSymbol.POP.value
                    daily_colour_df.iat[-1, 0] = ScatterColour.BLUE.value
                    daily_description_df = get_candle_description_df(daily_df.loc[:, idx[[ticker], :]])
                    
                    daily_chart_dir = get_candlestick_chart(pattern='INITIAL_POP', 
                                                            main_df=daily_df,
                                                            scatter_symbol_df=daily_symbol_df,
                                                            scatter_colour_df=daily_colour_df,
                                                            description_df=daily_description_df,
                                                            bar_size=BarSize.ONE_DAY)

                    embed = discord.Embed(title=f'{ticker} is popping up {round(yesterday_close_to_last_pct, 2)}% at {pop_up_time_display}')
                    embed.add_field(name = 'Close:', value= f'${close}', inline = True)
                    embed.add_field(name = 'Previous Close:', value = f'${yesterday_close}', inline = True)
                    embed.add_field(name = chr(173), value = chr(173))
                    embed.add_field(name = 'Volume:', value = f'{volume}', inline = True)
                    embed.add_field(name = 'Total Volume:', value = f'{total_volume}', inline = True)
                    embed.add_field(name = chr(173), value = chr(173))
                    embed.set_image(url=f"attachment://{os.path.basename(minute_chart_dir)}")
                    embed.set_image(url=f"attachment://{os.path.basename(daily_chart_dir)}")
                    #embed.set_thumbnail(url=f"attachment://{os.path.basename(daily_chart_dir)}")
                    #embed.set_footer(text='testing footer 1234', icon_url=f"attachment://{os.path.basename(daily_chart_dir)}")
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
    