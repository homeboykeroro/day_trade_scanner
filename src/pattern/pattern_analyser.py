from abc import ABC

from model.discord.discord_scanner_message import DiscordScannerMessage

from utils.discord_message_record_util import check_if_message_sent, add_sent_message_record
from utils.collection_util import get_chunk_list
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

MAX_READ_OUT_MESSAGE_CHUNK_SIZE = 3

logger = Logger()

class PatternAnalyser(ABC):
    def __init__(self, discord_client, sqlite_connector) -> None:
        self.__discord_client = discord_client
        self.__sqlite_connector = sqlite_connector
    
    def analyse(self) -> None:
        return NotImplemented
    
    def check_if_message_sent(self, message: DiscordScannerMessage):
        return check_if_message_sent(self.__sqlite_connector, message.ticker, message.hit_scanner_datetime, message.pattern, message.bar_size)
        
    def send_notification(self, message_list: list):
        read_out_message_list = []
        
        if message_list:
            for message in message_list:
                if message.display_message or message.embed:
                    self.__discord_client.send_messages_to_channel(embed=message.embed, message=message.display_message, attachments=message.candle_chart_list, channel_type=DiscordChannel.DEVELOPMENT_TEST)
                
                if message.read_out_message:
                    read_out_message_list.append(message.read_out_message)
                
                add_sent_message_record(self.__sqlite_connector, [(message.ticker, message.hit_scanner_datetime, message.pattern, message.bar_size)])
                logger.log_debug_msg(f'Sending this discord message: {message.read_out_message}')
            
            read_out_message_chunk_list = get_chunk_list(read_out_message_list, MAX_READ_OUT_MESSAGE_CHUNK_SIZE)
            
            for chunk in read_out_message_chunk_list:
                read_out_message = '\n'.join(chunk)
                self.__discord_client.send_messages_to_channel(message=read_out_message, channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            
