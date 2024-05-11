import threading
import time

from module.discord_chatbot_client import DiscordChatBotClient

from scanner import Scanner

from utils.datetime_util import is_within_trading_day_and_hours
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.discord.discord_channel import DiscordChannel

logger = Logger()

MAX_RETRY_CONNECTION_TIMES = 5
CONNECTION_FAIL_RETRY_INTERVAL = 10

SCANNER_REFRESH_INTERVAL = 5

class StockScreener(threading.Thread):
    def __init__(self, discord_client: DiscordChatBotClient):
        self.__discord_client = discord_client
        super().__init__()

    def scan(self):
        self.__scanner = Scanner(self.__discord_client)
        start_scan = False
        
        while True: 
            start_scan = is_within_trading_day_and_hours()
            
            if start_scan:
                break

            logger.log_debug_msg('Scanner is idle until valid trading weekday and time', with_std_out=True)
            time.sleep(SCANNER_REFRESH_INTERVAL)
    
        logger.log_debug_msg('Scanner is ready', with_std_out=True)
        logger.log_debug_msg('Start scanning', with_std_out=True)
        self.__discord_client.send_message_by_list_with_response([DiscordMessage(content='Stock screener started')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                
        self.__scanner.scan_intra_day_top_gainer()
        self.__scanner.scan_intra_day_top_loser()
        self.__scanner.scan_multi_days_top_gainer()
        self.__scanner.scan_yesterday_top_gainer()
 
    