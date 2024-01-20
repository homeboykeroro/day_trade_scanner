from abc import ABC, abstractmethod

from sql.sqlite_connector import SqliteConnector

from utils.discord_message_record_util import check_if_message_sent, add_sent_message_record
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

logger = Logger()

class PatternAnalyser(ABC):
    def __init__(self, discord_client, sqllite_connector) -> None:
        self.__discord_client = discord_client
        self.__sqllite_connector = sqllite_connector
    
    def analyse(self) -> None:
        return NotImplemented
        
    def send_notification(self, message_list: list):
        if message_list:
            for message in message_list:
                if not self.__sqllite_connector:
                   logger.log_error_msg('Sqlite connection error, re-create new SQLite connection', with_std_out=True)
                   self.__sqllite_connector = SqliteConnector()
                    
                is_message_sent = check_if_message_sent(self.__sqllite_connector, message.ticker, message.hit_scanner_datetime, message.pattern, message.bar_size)
                if not is_message_sent:
                    if message.display_message or message.embed:
                        self.__discord_client.send_messages_to_channel(embed=message.embed, message=message.display_message, channel=DiscordChannel.DEVELOPMENT_TEST)
                    if message.read_out_message:
                        self.__discord_client.send_messages_to_channel(message=message.read_out_message, channel=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    
                    add_sent_message_record(self.__sqllite_connector, [(message.ticker, message.hit_scanner_datetime, message.pattern, message.bar_size)])
                    logger.log_debug_msg(f'Sending this discord message: {message}')
                else:
                    logger.log_debug_msg(f'The discord message had already been sent before: {message}')
                        
