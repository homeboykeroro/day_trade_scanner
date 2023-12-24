import math
import time
import threading
from requests import RequestException
from aiohttp import ClientError

from pattern.pattern_analyser_factory import PatternAnalyserFactory

from utils.datetime_util import is_within_trading_day_and_hours
from utils.market_data_util import get_scanner_result, get_contract_minute_candle_data, check_auth_status
from utils.discord_message_record_util import check_if_message_sent, delete_all_sent_message_record, add_sent_message_record
from utils.logger import Logger

from constant.pattern import Pattern
from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

from module.discord_chatbot_client import DiscordChatBotClient

from sql.sqlite_connector import SqliteConnector

from exception.sqlite_connection_error import SqliteConnectionError

logger = Logger()

SCANNER_REFRESH_INTERVAL = 30
CONNECTION_FAIL_RETRY_INTERVAL = 60
MAX_NO_OF_SCANNER_RESULT = 15

PATTERN_TO_CANDLE_DATA_DICT = {
    Pattern.INITIAL_POP_UP: [BarSize.ONE_MINUTE],
    Pattern.NEW_FIVE_MINUTE_CANDLE_HIGH: [BarSize.FIVE_MINUTE]
}

class Scanner():
    def __init__(self, discord_client: DiscordChatBotClient):
        self.__discord_client = discord_client
        self.__is_scanner_idle = False
        self.__is_scanner_ready = False
        self.__ib_connection_retry = False
        self.__fatal_error = False
        self.__ticker_to_previous_day_data_dict = {}
        self.__ticker_to_contract_dict = {}
        self.__historical_candle_data_dict = {}
        
    def __scan(self):
        self.__sqllite_connector = SqliteConnector()
        
        while True: 
            if self.__is_scanner_ready:
                scan_start_time = time.time()
                logger.log_debug_msg('Start scanning', with_std_out=True)
                
                try:
                    check_auth_status()
                    
                    scanner_result_list = get_scanner_result(MAX_NO_OF_SCANNER_RESULT)
                    self.__historical_candle_data_dict = get_contract_minute_candle_data(scanner_result_list, self.__ticker_to_previous_day_data_dict, self.__ticker_to_contract_dict)

                    if self.__historical_candle_data_dict:
                        for pattern, bar_size_list in PATTERN_TO_CANDLE_DATA_DICT.items():
                            for bar_size in bar_size_list:
                                candle_df = self.__historical_candle_data_dict[bar_size]
                                pattern_analyser = PatternAnalyserFactory.get_pattern_analyser(pattern, candle_df, self.__ticker_to_contract_dict)
                                analysed_result_message_list = pattern_analyser.analyse()

                                if analysed_result_message_list:
                                    for message in analysed_result_message_list:
                                        if not self.__sqllite_connector:
                                           logger.log_error_msg('Sqlite connection error, re-create new SQLite connection', with_std_out=True)
                                           self.__sqllite_connector = SqliteConnector()
                                            
                                        is_message_sent = check_if_message_sent(self.__sqllite_connector, (message.ticker, message.hit_scanner_datetime, pattern, bar_size))
                                        is_message_sent = True
                                        if not is_message_sent:
                                            if message.display_message:
                                                self.__discord_client.send_messages_to_channel(message, DiscordChannel.DAY_TRADE_FLOOR)
                                            if message.read_out_message:
                                                self.__discord_client.send_messages_to_channel(message, DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                                                
                                            add_sent_message_record(self.__sqllite_connector, [(message.ticker, message.hit_scanner_datetime, pattern, bar_size)])
                except (RequestException, ClientError) as connection_exception:
                    self.__ib_connection_retry = True
                    self.__discord_client.send_messages_to_channel('Client Portal API connection failed', DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'Client Portal API connection error, {connection_exception}', with_std_out=True)
                except (SqliteConnectionError) as sqlite_connection_exception:
                    self.__discord_client.send_messages_to_channel('SQLite connection error', DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'SQLite connection error, {sqlite_connection_exception}', with_std_out=True)
                except Exception as exception:
                    self.__fatal_error = True   
                    self.__discord_client.send_messages_to_channel('Fatal error', DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
                    logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)

                self.__wait_till_next_scan(scan_start_time)
            else:
                if is_within_trading_day_and_hours():
                    self.__is_scanner_ready = True
                    logger.log_debug_msg('Scanner is ready', with_std_out=True)
                else:
                    if not self.__is_scanner_idle:
                        self.__is_scanner_idle = True
                        logger.log_debug_msg('Scanner is idle until valid trading weekday and time', with_std_out=True)
                        logger.log_debug_msg('Delete previously sent message record', with_std_out=True)
                        no_of_record_delete = delete_all_sent_message_record(self.__sqllite_connector)
                        logger.log_debug_msg(f'{no_of_record_delete} records has been sucessfully deleted', with_std_out=True)
                    time.sleep(SCANNER_REFRESH_INTERVAL)
    
    def __wait_till_next_scan(self, scan_start_time):
        refresh_interval = None 
        
        if self.__ib_connection_retry:
            logger.log_debug_msg(f'Retry client portal API connection after: {CONNECTION_FAIL_RETRY_INTERVAL}', with_std_out=True)
            refresh_interval = CONNECTION_FAIL_RETRY_INTERVAL
        
        if self.__fatal_error:
            logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_REFRESH_INTERVAL}', with_std_out=True)
            refresh_interval = SCANNER_REFRESH_INTERVAL
  
        scan_time = time.time() - scan_start_time
        
        if (not self.__ib_connection_retry and not self.__fatal_error) and (scan_time < SCANNER_REFRESH_INTERVAL):
            logger.log_debug_msg(f'Refresh scanner after: {math.ceil(SCANNER_REFRESH_INTERVAL - scan_time)}', with_std_out=True)
            refresh_interval = math.ceil(SCANNER_REFRESH_INTERVAL - scan_time)
            
        logger.log_debug_msg(f'Scan time taken: {scan_time}', with_std_out=True)
            
        if refresh_interval:   
            time.sleep(refresh_interval)
        
        self.__ib_connection_retry = False
        self.__fatal_error = False

    def run_scanner(self) -> None:
        scanner_thread = threading.Thread(target=self.__scan, name="monitor_thread")
        scanner_thread.start()
            