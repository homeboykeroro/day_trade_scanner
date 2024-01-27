import time
import threading

from module.discord_chatbot_client import DiscordChatBotClient

from scanner import Scanner

from datasource.ib_connector import IBConnector
from datasource.finviz_connector import FinvizConnector

from sql.sqlite_connector import SqliteConnector

from utils.datetime_util import is_within_trading_day_and_hours
from utils.discord_message_record_util import delete_all_sent_message_record
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

from aiohttp import ClientError
from requests import RequestException
from exception.sqlite_connection_error import SqliteConnectionError

logger = Logger()

SCANNER_REFRESH_INTERVAL = 30
CONNECTION_FAIL_RETRY_INTERVAL = 60

class StockScreener():
    def __init__(self, discord_client: DiscordChatBotClient):
        self.__discord_client = discord_client
        self.__is_scanner_idle = False
        self.__ib_connection_retry = False
        self.__fatal_error = False
        
        self.__ib_connector = IBConnector()
        self.__finviz_connector = FinvizConnector()
        
        self.__ib_connector.check_auth_status()
        self.__ib_connector.receive_brokerage_account()
        
    def __scan(self):
        self.__sqllite_connector = SqliteConnector()
        self.__clean_sent_discord_message_record()
        self.__scanner = Scanner(self.__discord_client, self.__ib_connector, self.__finviz_connector, self.__sqllite_connector)
        
        while True: 
            if is_within_trading_day_and_hours():
                scan_start_time = time.time()
                logger.log_debug_msg('Start scanning', with_std_out=True)
                
                try:
                    self.__scanner.scan_top_gainer()
                    #self.__scanner.scan_yesterday_top_gainer()
                    #self.__scanner.scan_top_loser()
                    logger.log_debug_msg(f'Scan time taken: {time.time() - scan_start_time}') 
                except (RequestException, ClientError) as connection_exception:
                    self.__ib_connection_retry = True
                    self.__discord_client.send_messages_to_channel(message='Client Portal API connection failed', channel=DiscordChannel.CHATBOT_LOG, with_text_to_speech=True)
                    logger.log_error_msg(f'Client Portal API connection error, {connection_exception}', with_std_out=True)
                except (SqliteConnectionError) as sqlite_connection_exception:
                    self.__discord_client.send_messages_to_channel(message='SQLite connection error', channel=DiscordChannel.CHATBOT_LOG, with_text_to_speech=True)
                    logger.log_error_msg(f'SQLite connection error, {sqlite_connection_exception}', with_std_out=True)
                except Exception as exception:
                    self.__fatal_error = True   
                    self.__discord_client.send_messages_to_channel(message='Fatal error', channel=DiscordChannel.CHATBOT_LOG, with_text_to_speech=True)   
                    logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)

                self.__wait_till_next_scan()
            else:
                if not self.__is_scanner_idle:
                    logger.log_debug_msg('Scanner is ready', with_std_out=True)
                    logger.log_debug_msg('Scanner is idle until valid trading weekday and time', with_std_out=True)
                    self.__is_scanner_idle = True
                
                time.sleep(SCANNER_REFRESH_INTERVAL)
    
    def __wait_till_next_scan(self):
        refresh_interval = None 
        
        if self.__ib_connection_retry:
            logger.log_debug_msg(f'Retry client portal API connection after: {CONNECTION_FAIL_RETRY_INTERVAL}', with_std_out=True)
            refresh_interval = CONNECTION_FAIL_RETRY_INTERVAL
        
        if self.__fatal_error:
            logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_REFRESH_INTERVAL}', with_std_out=True)
            refresh_interval = SCANNER_REFRESH_INTERVAL

        if refresh_interval:   
            time.sleep(refresh_interval)
        
        self.__ib_connection_retry = False
        self.__fatal_error = False
        
    def __clean_sent_discord_message_record(self):
        logger.log_debug_msg('Delete previously sent message record', with_std_out=True)
        no_of_record_delete = delete_all_sent_message_record(self.__sqllite_connector)
        logger.log_debug_msg(f'{no_of_record_delete} records has been sucessfully deleted', with_std_out=True)

    def run_screener(self) -> None:
        scanner_thread = threading.Thread(target=self.__scan, name="monitor_thread")
        scanner_thread.start()
            