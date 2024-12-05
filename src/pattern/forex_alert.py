import time
import pandas as pd
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from model.discord.discord_message import DiscordMessage

from constant.candle.bar_size import BarSize
from constant.discord.discord_message_channel import DiscordMessageChannel
from constant.indicator.indicator import Indicator
from constant.indicator.runtime_indicator import RuntimeIndicator

from utils.common.dataframe_util import get_ticker_to_occurrence_idx_list
from utils.common.datetime_util import convert_into_human_readable_time, convert_into_read_out_time
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class ForexAlert(PatternAnalyser):    
    def __init__(self, bar_size: BarSize, minute_candle_df: DataFrame, 
                       min_trigger_price: int,
                       max_trigger_price: int,
                       pattern_name,
                       discord_client):
        super().__init__(discord_client)
        self.__bar_size = bar_size
        self.__minute_candle_df = minute_candle_df
        self.__min_trigger_price = min_trigger_price
        self.__max_trigger_price = max_trigger_price
        self.__pattern_name = pattern_name
    
    def analyse(self) -> None:
        truncated_decimal_place_low_df = (self.__minute_candle_df.loc[:, idx[:, Indicator.LOW.value]]
                                                                 .rename(columns={Indicator.LOW.value: RuntimeIndicator.COMPARE.value})
                                                                 .applymap(lambda x: int(x * 100) / 100))
        truncated_decimal_place_high_df = (self.__minute_candle_df.loc[:, idx[:, Indicator.HIGH.value]]
                                                                  .rename(columns={Indicator.HIGH.value: RuntimeIndicator.COMPARE.value})
                                                                  .applymap(lambda x: int(x * 100) / 100))
        
        trigger_price_boolean_df = (truncated_decimal_place_low_df >= self.__min_trigger_price) & (truncated_decimal_place_high_df <= self.__max_trigger_price)
        trigger_price_boolean_series = trigger_price_boolean_df.any()
        trigger_price_forex_list = trigger_price_boolean_series.index[trigger_price_boolean_series].get_level_values(0).tolist()
        
        if len(trigger_price_forex_list) > 0:
            ticker_to_occurrence_idx_list_dict = get_ticker_to_occurrence_idx_list(trigger_price_boolean_df)
            
            for ticker in trigger_price_forex_list:
                occurrence_idx_list = []
                if len(ticker_to_occurrence_idx_list_dict) > 0:
                    occurrence_idx_list = ticker_to_occurrence_idx_list_dict[ticker]
                
                if len(occurrence_idx_list) > 0:
                    last_trigger_datetime = occurrence_idx_list[-1]
                    datetime_display = convert_into_human_readable_time(last_trigger_datetime)
                    read_out_pop_up_time = convert_into_read_out_time(last_trigger_datetime)
                    
                    is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, 
                                                                                  hit_scanner_datetime=last_trigger_datetime.replace(second=0, microsecond=0), 
                                                                                  pattern=self.__pattern_name, 
                                                                                  bar_size=self.__bar_size)
                    
                    if not is_message_sent:
                        close = self.__minute_candle_df.loc[:, (ticker, Indicator.CLOSE.value)]
                        alert_msg = f'{ticker} trigger price alert (${self.__min_trigger_price} - ${self.__max_trigger_price}) at {datetime_display}, current close: ${close}'
                        readout_msg = f'{ticker} trigger place alert ranged from ${self.__min_trigger_price} to ${self.__max_trigger_price} at {read_out_pop_up_time}'
                        trigger_alert_message = DiscordMessage(content=alert_msg)
        
                        send_msg_start_time = time.time()
                        response = self._discord_client.send_message_by_list_with_response(message_list=[trigger_alert_message], channel_type=DiscordMessageChannel.FOREX)

                        if len(response) > 0:
                            if not hasattr(response[0], 'embeds'):
                                self._discord_client.send_message(message=DiscordMessage(content=f'Failed to send message to {DiscordMessageChannel.FOREX.value}, {response[0]}'), channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG)
                                raise Exception(f'Failed to send message to {DiscordMessageChannel.FOREX.value}, {response[0]}')

                            jump_url = response[0].jump_url
                            notification_message = DiscordMessage(ticker=ticker, jump_url=jump_url, content=readout_msg)
                            self._discord_client.send_message(message=notification_message, channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                        else:  
                            self._discord_client.send_message(message=DiscordMessage(content=f'No response returned from message {trigger_alert_message} to {DiscordMessageChannel.FOREX.value}'), channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG)

                        logger.log_debug_msg(f'{self.__pattern_name} send message time: {time.time() - send_msg_start_time} seconds')