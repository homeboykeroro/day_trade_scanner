from abc import ABC
import datetime

from model.discord.discord_message import DiscordMessage

from utils.discord_message_record_util import check_if_pattern_analysis_message_sent, add_sent_pattern_analysis_message_record
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel
from constant.candle.bar_size import BarSize

MAX_READ_OUT_MESSAGE_CHUNK_SIZE = 3

logger = Logger()

class PatternAnalyser(ABC):
    def __init__(self, discord_client, db_connector) -> None:
        self.__discord_client = discord_client
        self.__db_connector = db_connector
    
    def analyse(self) -> None:
        return NotImplemented
    
    def check_if_pattern_analysis_message_sent(self, ticker: str, hit_scanner_datetime: datetime, pattern: str, bar_size: BarSize):
        return check_if_pattern_analysis_message_sent(self.__db_connector, ticker, hit_scanner_datetime, pattern, bar_size.value)
        
    def send_notification(self, scanner_result_list: list, discord_channel: DiscordChannel, is_async: bool = True):
        if scanner_result_list:
            save_notification_db_record_param_list = []
            title_to_read_out_message_dict = {}
            title_to_ticker_dict = {}
            
            for scanner_result in scanner_result_list:
                title = scanner_result.embed.title
                title_to_read_out_message_dict[title] = scanner_result.readout_msg
                title_to_ticker_dict[title] = scanner_result.ticker
                
                notification_db_record_parms = [scanner_result.ticker, scanner_result.hit_scanner_datetime, scanner_result.pattern, scanner_result.bar_size]
                save_notification_db_record_param_list.append(notification_db_record_parms)
            
            if is_async:
                response_list = self.__discord_client.send_message_by_list_with_response(message_list=scanner_result_list, channel_type=discord_channel)
            else:
                response_list = []
                for scanner_result in scanner_result_list:
                    individual_msg_response_list = self.__discord_client.send_message_by_list_with_response(message_list=[scanner_result], channel_type=discord_channel)
                    response_list.append(individual_msg_response_list[0])

            notification_message_list = []
            for response in response_list:
                title = response.embeds[0].title   
                jump_url = response.jump_url
                readout_msg = title_to_read_out_message_dict[title]
                ticker = title_to_ticker_dict[title]
                message = DiscordMessage(ticker=ticker, jump_url=jump_url, content=readout_msg)
                notification_message_list.append(message)
            
            self.__discord_client.send_message_by_list_with_response(message_list=notification_message_list, channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            
            for save_notification in save_notification_db_record_param_list:
                add_sent_pattern_analysis_message_record(self.__db_connector, [save_notification])
