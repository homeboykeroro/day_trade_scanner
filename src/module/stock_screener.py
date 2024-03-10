import time
import traceback
import threading

from module.discord_chatbot_client import DiscordChatBotClient

from scanner import Scanner

from datasource.ib_connector import IBConnector
from datasource.finviz_connector import FinvizConnector

from sql.sqlite_connector import SqliteConnector

from model.discord.discord_message import DiscordMessage

from utils.datetime_util import is_within_trading_day_and_hours
from utils.discord_message_record_util import delete_all_sent_pattern_analysis_message_record
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

from aiohttp import ClientError
from requests import RequestException
from exception.sqlite_connection_error import SqliteConnectionError

logger = Logger()

MAX_RETRY_CONNECTION_TIMES = 5
SCANNER_REFRESH_INTERVAL = 5
CONNECTION_FAIL_RETRY_INTERVAL = 10

class StockScreener():
    def __init__(self, discord_client: DiscordChatBotClient):
        self.__discord_client = discord_client
        self.__is_scanner_idle = False
        
        self.__ib_connector = IBConnector()
        self.__finviz_connector = FinvizConnector()
        
        try:
            self.__ib_connector.check_auth_status()
            self.__ib_connector.receive_brokerage_account()
        except Exception as preflight_request_exception:
            self.__discord_client.send_message(DiscordMessage(content='Client Portal API preflight requests failed, re-authenticating seesion'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            logger.log_error_msg(f'Client Portal API preflight requests error, {preflight_request_exception}', with_std_out=True)
            self.__reauthenticate()
            
    def __scan(self):
        self.__sqllite_connector = SqliteConnector()
        self.__scanner = Scanner(self.__discord_client, self.__ib_connector, self.__finviz_connector, self.__sqllite_connector)
        self.__clean_sent_discord_message_record()
        
        while True: 
            if is_within_trading_day_and_hours():
                scan_start_time = time.time()
                logger.log_debug_msg('Start scanning', with_std_out=True)
                
                try:
                    self.__scanner.scan_top_gainer()
                    #self.__scanner.scan_yesterday_top_gainer()
                    self.__scanner.scan_top_loser()
                    logger.log_debug_msg(f'Scan time taken: {time.time() - scan_start_time}') 
                except (RequestException, ClientError) as connection_exception:
                    self.__discord_client.send_message(DiscordMessage(content='Client Portal API connection failed, re-authenticating seesion'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'Client Portal API connection error, {connection_exception}', with_std_out=True)
                    self.__reauthenticate()
                except (SqliteConnectionError) as sqlite_connection_exception:
                    self.__discord_client.send_message(DiscordMessage(content='SQLite connection error'), channel_type=DiscordChannel.CHATBOT_LOG, with_text_to_speech=True)
                    logger.log_error_msg(f'SQLite connection error, {sqlite_connection_exception}', with_std_out=True)
                except Exception as exception:
                    self.__discord_client.send_message(DiscordMessage(content='Fatal error'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
                    self.__discord_client.send_message(DiscordMessage(content=traceback.format_exc()), channel_type=DiscordChannel.CHATBOT_LOG)
                    logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)
                    logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_REFRESH_INTERVAL} seconds', with_std_out=True)
                    refresh_interval = SCANNER_REFRESH_INTERVAL 
                    time.sleep(refresh_interval)
            else:
                if not self.__is_scanner_idle:
                    logger.log_debug_msg('Scanner is ready', with_std_out=True)
                    logger.log_debug_msg('Scanner is idle until valid trading weekday and time', with_std_out=True)
                    self.__is_scanner_idle = True
                    #self.__clean_sent_discord_message_record()
                
                time.sleep(SCANNER_REFRESH_INTERVAL)
    
    def __reauthenticate(self):
        retry_times = 0
        
        while True:
            try:
                self.__ib_connector.reauthenticate()
            except Exception as reauthenticate_exception:
                if retry_times < MAX_RETRY_CONNECTION_TIMES:
                    self.__discord_client.send_message(DiscordMessage(content=f'Failed to re-authenticate session, Retry reauthentication after {CONNECTION_FAIL_RETRY_INTERVAL} seconds'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                    retry_times += 1
                    time.sleep(CONNECTION_FAIL_RETRY_INTERVAL)
                    continue
                else:
                    self.__discord_client.send_message(DiscordMessage(content=f'Maximum re-authentication attemps exceed. Please restart application'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    exit(1)
            break
        
    def __clean_sent_discord_message_record(self):
        logger.log_debug_msg('Delete previously sent message record', with_std_out=True)
        no_of_record_delete = delete_all_sent_pattern_analysis_message_record(self.__sqllite_connector)
        logger.log_debug_msg(f'{no_of_record_delete} records has been sucessfully deleted', with_std_out=True)

    def run_screener(self) -> None:
        scanner_thread = threading.Thread(target=self.__scan, name="monitor_thread")
        scanner_thread.start()
            