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

from utils.dataframe_util import derive_idx_df
from utils.datetime_util import convert_into_human_readable_time, convert_into_read_out_time

from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class InitialDip(PatternAnalyser):
    MAX_GAP_DOWN_PCT = -8
    MAX_YESTERDAY_CLOSE_TO_LAST_PCT = -10
        
    def __init__(self, bar_size: BarSize, historical_data_df: DataFrame, yesterday_daily_candle_df: DataFrame, ticker_to_contract_info_dict: dict, discord_client, sqllite_connector):
        super().__init__(discord_client, sqllite_connector)
        self.__bar_size = bar_size
        self.__historical_data_df = historical_data_df
        self.__yesterday_daily_candle_df = yesterday_daily_candle_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> list:
        message_list = []
        logger.log_debug_msg('Initial dip scan')
        start_time = time.time()
        
        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]]
        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
        
        yesterday_close_df = self.__yesterday_daily_candle_df.loc[:, idx[:, Indicator.CLOSE.value]]
        yesterday_close_to_last_pct_df = (close_df.sub(yesterday_close_df.values)
                                                  .div(yesterday_close_df.values)
                                                  .mul(100))

        yesterday_lower_body_df = self.__yesterday_daily_candle_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_BODY.value]]
        upper_body_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_UPPER_BODY.value]]
        gap_down_pct_df = (upper_body_df.sub(yesterday_lower_body_df.values)
                                      .div(yesterday_lower_body_df.values)
                                      .mul(100))
        
        max_gap_down_pct_df = (gap_down_pct_df <= self.MAX_GAP_DOWN_PCT).rename(columns={CustomisedIndicator.CANDLE_UPPER_BODY.value: RuntimeIndicator.COMPARE.value})
        max_yesterday_close_to_last_pct_boolean_df = (yesterday_close_to_last_pct_df <= self.MAX_YESTERDAY_CLOSE_TO_LAST_PCT).rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
        red_candle_candle_df = (candle_colour_df == CandleColour.RED.value).rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        dip_boolean_df = (max_gap_down_pct_df) & (max_yesterday_close_to_last_pct_boolean_df) & (red_candle_candle_df)
        numeric_idx_df = derive_idx_df(dip_boolean_df)
        first_occurrence_idx_np = numeric_idx_df.where(dip_boolean_df.values).idxmin().values
        first_dip_occurrence_df = (numeric_idx_df == first_occurrence_idx_np)
        
        datetime_idx_df = derive_idx_df(dip_boolean_df, numeric_idx=False)
        dip_datetime_idx_df = datetime_idx_df.where(first_dip_occurrence_df.values).ffill()
        result_boolean_df = dip_datetime_idx_df.notna()
        
        new_loser_result_series = result_boolean_df.any()   
        new_loser_ticker_list = new_loser_result_series.index[new_loser_result_series].get_level_values(0).tolist()
        
        logger.log_debug_msg(f'Initial pop analysis time: {time.time() - start_time} seconds')

        if len(new_loser_ticker_list) > 0:
            send_msg_start_time = time.time()
            
            for ticker in new_loser_ticker_list:
                contract_info = self.__ticker_to_contract_info_dict[ticker]
                
                dip_close_df = (self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE.value]]
                                                            .where(first_dip_occurrence_df.values)
                                                            .ffill())
                dip_yesterday_close_df = yesterday_close_df.loc[:, idx[[ticker], Indicator.CLOSE.value]]
                dip_yesterday_close_to_last_pct_df = (yesterday_close_to_last_pct_df.where(first_dip_occurrence_df.values)
                                                                              .ffill())
                dip_volume_df = (self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME.value]]
                                                             .where(first_dip_occurrence_df.values)
                                                             .ffill())
                dip_total_volume_df = (self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.TOTAL_VOLUME.value]]
                                                                   .where(first_dip_occurrence_df.values)
                                                                   .ffill())
                
                close = dip_close_df.loc[dip_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                yesterday_close = dip_yesterday_close_df.loc[dip_yesterday_close_df.index[-1], (ticker, Indicator.CLOSE.value)]
                yesterday_close_to_last_pct = dip_yesterday_close_to_last_pct_df.loc[dip_yesterday_close_to_last_pct_df.index[-1], (ticker, Indicator.CLOSE.value)]
                volume = "{:,}".format(int(dip_volume_df.loc[dip_volume_df.index[-1], (ticker, Indicator.VOLUME.value)]))
                total_volume = "{:,}".format(int(dip_total_volume_df.loc[dip_total_volume_df.index[-1], (ticker, CustomisedIndicator.TOTAL_VOLUME.value)]))
                dip_time = dip_datetime_idx_df.loc[dip_datetime_idx_df.index[-1], (ticker, RuntimeIndicator.INDEX.value)]
                dip_time_display = convert_into_human_readable_time(dip_time)
                read_out_dip_time = convert_into_read_out_time(dip_time)
                
                embed = discord.Embed(title=f'{ticker} is sinking {round(yesterday_close_to_last_pct, 2)}% at {dip_time_display}')
                embed.add_field(name = 'Close:', value= f'${close}', inline = True)
                embed.add_field(name = 'Previous Close:', value = f'{yesterday_close}', inline = True)
                embed.add_field(name = chr(173), value = chr(173))
                embed.add_field(name = 'Volume:', value = f'{volume}', inline = True)
                embed.add_field(name = 'Total Volume:', value = f'{total_volume}', inline = True)
                embed.add_field(name = chr(173), value = chr(173))
                contract_info.add_contract_info_to_embed_msg(embed)
                
                read_out_ticker = " ".join(ticker)
                read_out_message = f'{read_out_ticker} is sinking {round(yesterday_close_to_last_pct, 2)} percent at {read_out_dip_time}'
                
                message = DiscordScannerMessage(embed=embed, 
                                                display_message=None, read_out_message=read_out_message, 
                                                ticker=ticker, 
                                                hit_scanner_datetime=dip_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                pattern='INITIAL_DIP',
                                                bar_size=self.__bar_size.value)
                message_list.append(message)
                
            logger.log_debug_msg(f'Initial dip send message time: {time.time() - send_msg_start_time} seconds')
         
        if message_list:       
            self.send_notification(message_list)