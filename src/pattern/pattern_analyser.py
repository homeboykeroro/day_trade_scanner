from abc import ABC
import datetime

import discord

from model.discord.discord_message import DiscordMessage
from model.discord.view.redirect_button import RedirectButton

from utils.discord_message_record_util import check_if_message_sent, add_sent_message_record
from utils.collection_util import get_chunk_list
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel
from constant.candle.bar_size import BarSize

MAX_READ_OUT_MESSAGE_CHUNK_SIZE = 3

logger = Logger()

class PatternAnalyser(ABC):
    def __init__(self, discord_client, sqlite_connector) -> None:
        self.__discord_client = discord_client
        self.__sqlite_connector = sqlite_connector
    
    def analyse(self) -> None:
        return NotImplemented
    
    def check_if_message_sent(self, ticker: str, hit_scanner_datetime: datetime, pattern: str, bar_size: BarSize):
        return check_if_message_sent(self.__sqlite_connector, ticker, hit_scanner_datetime.strftime('%Y-%m-%d %H:%M:%S'), pattern, bar_size.value)
        
    def send_notification(self, scanner_result_list: list, discord_channel: DiscordChannel):
        if scanner_result_list:
            title_to_read_out_message_dict = {}
            title_to_ticker_dict = {}
            
            for scanner_result in scanner_result_list:
                title = scanner_result.embed.title
                title_to_read_out_message_dict[title] = scanner_result.readout_msg
                title_to_ticker_dict[title] = scanner_result.ticker
            
            response_list = self.__discord_client.send_message_by_list_with_response(message_list=scanner_result_list, channel_type=discord_channel)

            notification_message_list = []
            for response in response_list:
                title = response.embeds[0].title   
                jump_url = response.jump_url
                readout_msg = title_to_read_out_message_dict[title]
                ticker = title_to_ticker_dict[title]
                message = DiscordMessage(ticker=ticker, jump_url=jump_url, content=readout_msg)
                notification_message_list.append(message)
                
            self.__discord_client.send_message_by_list_with_response(message_list=notification_message_list, channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
