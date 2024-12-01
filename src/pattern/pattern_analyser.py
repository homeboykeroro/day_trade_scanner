from abc import ABC
import datetime

from model.discord.discord_message import DiscordMessage

from utils.sql.discord_message_record_util import check_if_pattern_analysis_message_sent, check_if_pattern_analysis_message_sent_by_daily_basis ,add_sent_pattern_analysis_message_record
from utils.logger import Logger

from constant.discord.discord_message_channel import DiscordMessageChannel
from constant.candle.bar_size import BarSize

MAX_READ_OUT_MESSAGE_CHUNK_SIZE = 3

logger = Logger()

class PatternAnalyser(ABC):
    def __init__(self, discord_client) -> None:
        self._discord_client = discord_client
    
    def analyse(self) -> None:
        return NotImplemented
    
    def check_if_pattern_analysis_message_sent_by_daily_basis(self, ticker: str, hit_scanner_datetime: datetime.datetime, pattern: str, bar_size: BarSize, max_occurrence: int):
        return check_if_pattern_analysis_message_sent_by_daily_basis(ticker, hit_scanner_datetime, pattern, bar_size.value, max_occurrence)
    
    def check_if_pattern_analysis_message_sent(self, ticker: str, hit_scanner_datetime: datetime.datetime, pattern: str, bar_size: BarSize):
        return check_if_pattern_analysis_message_sent(ticker, hit_scanner_datetime, pattern, bar_size.value)
        
    def send_notification(self, scanner_result_list: list, discord_channel: DiscordMessageChannel, is_async: bool = True):
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
                response_list = self._discord_client.send_message_by_list_with_response(message_list=scanner_result_list, channel_type=discord_channel)
            else:
                response_list = []
                for scanner_result in scanner_result_list:
                    individual_msg_response_list = self._discord_client.send_message_by_list_with_response(message_list=[scanner_result], channel_type=discord_channel)
                    response_list.append(individual_msg_response_list[0])

            notification_message_list = []
            send_notification_ticker_list = []
            invalid_notification_ticker_list = []
            for response in response_list:
                if not hasattr(response, 'embeds'):
                    self._discord_client.send_message(DiscordMessage(content=f'Failed to send message {str(response)} to {discord_channel.value}'), DiscordMessageChannel.CHATBOT_ERROR_LOG)
                    continue
                
                title = response.embeds[0].title   
                jump_url = response.jump_url
                readout_msg = title_to_read_out_message_dict[title]
                ticker = title_to_ticker_dict[title]
                message = DiscordMessage(ticker=ticker, jump_url=jump_url, content=readout_msg)
                notification_message_list.append(message)
                send_notification_ticker_list.append(ticker)
            
            for scanner_result in scanner_result_list:
                scanner_ticker = scanner_result.ticker
                if scanner_ticker not in send_notification_ticker_list:
                    invalid_notification_ticker_list.append(scanner_ticker)
            
            if invalid_notification_ticker_list:
                self._discord_client.send_message(DiscordMessage(content=f'Failed to send notification to {discord_channel.value} channel, ticker list: {str(invalid_notification_ticker_list)}'), DiscordMessageChannel.CHATBOT_ERROR_LOG)
            
            if is_async:
                self._discord_client.send_message_by_list_with_response(message_list=notification_message_list, channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            else:
                for notification_message in notification_message_list:
                    self._discord_client.send_message_by_list_with_response(message_list=[notification_message], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            
            add_sent_pattern_analysis_message_record(save_notification_db_record_param_list)
