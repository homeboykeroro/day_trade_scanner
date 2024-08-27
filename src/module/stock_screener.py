import os
import threading
import time
import traceback
import oracledb
from aiohttp import ClientError
from requests import HTTPError, RequestException

from datasource.ib_connector import IBConnector

from module.discord_chatbot_client import DiscordChatBotClient

from scanner import Scanner

from utils.config_util import get_config
from utils.datetime_util import is_within_trading_day_and_hours
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.discord.discord_channel import DiscordChannel

logger = Logger()

STACKTRACE_CHUNK_SIZE = get_config('SYS_PARAM', 'STACKTRACE_CHUNK_SIZE')
MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES = get_config('SYS_PARAM', 'MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES')
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_FATAL_ERROR_REFRESH_INTERVAL')
SCANNER_IDLE_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_IDLE_REFRESH_INTERVAL')
SCANNER_REAUTHENTICATION_RETRY_INTERVAL = get_config('SYS_PARAM', 'SCANNER_REAUTHENTICATION_RETRY_INTERVAL')

class StockScreener(threading.Thread):
    def __init__(self, discord_client: DiscordChatBotClient):
        self.__discord_client = discord_client
        self.__reauthentication_retry_times = 0
        super().__init__()

    def __reauthenticate(self, ib_connector: IBConnector):
        while True:
            try:
                if self.__reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                    logger.log_debug_msg('Send reauthenticate requests', with_std_out=True)
                    ib_connector.reauthenticate()
                else:
                    raise RequestException("Reauthentication failed")
            except (RequestException, ClientError, HTTPError)  as reauthenticate_exception:
                if self.__reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                    self.__discord_client.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session, retry after {SCANNER_REAUTHENTICATION_RETRY_INTERVAL} seconds')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                    self.__reauthentication_retry_times += 1
                    time.sleep(SCANNER_REAUTHENTICATION_RETRY_INTERVAL)
                    continue
                else:
                    self.__discord_client.send_message(DiscordMessage(content=f'Maximum re-authentication attemps exceed. Please restart application'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    time.sleep(30)
                    os._exit(1)
            except Exception as exception:
                self.__discord_client.send_message(DiscordMessage(content=f'Re-authentication fatal error. Please restart application'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                time.sleep(30)
                os._exit(1)

            self.__reauthentication_retry_times = 0
            self.__discord_client.send_message_by_list_with_response([DiscordMessage(content='Reauthentication succeed')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            logger.log_debug_msg('Reauthentication succeed', with_std_out=True)
            break 

    def scan(self):
        self.__scanner = Scanner(self.__discord_client)
        start_scan = False
        
        while True: 
            start_scan = is_within_trading_day_and_hours()
            
            if start_scan:
                break

            logger.log_debug_msg('Scanner is idle until valid trading weekday and time', with_std_out=True)
            time.sleep(SCANNER_IDLE_REFRESH_INTERVAL)
    
        logger.log_debug_msg('Scanner is ready', with_std_out=True)
        logger.log_debug_msg('Start scanning', with_std_out=True)
        self.__discord_client.send_message_by_list_with_response([DiscordMessage(content='Stock screener started')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
         
        try:
            self.__scanner.send_ib_preflight_request()
        except (RequestException, ClientError, HTTPError) as connection_exception:
            logger.log_error_msg(f'Client portal API preflight request error, {connection_exception}', with_std_out=True)
            self.__reauthenticate(self.__scanner.ib_connector)
        
        while True:     
            try:
                self.__scanner.scan_intra_day_top_gainer()
                self.__scanner.scan_intra_day_top_loser()
                self.__scanner.scan_multi_days_top_gainer()
                self.__scanner.scan_yesterday_top_gainer()
            except (RequestException, ClientError, HTTPError) as connection_exception:
                logger.log_error_msg(f'Client portal API connection error, {connection_exception}', with_std_out=True)
                self.__reauthenticate(self.__scanner.ib_connector)
            except oracledb.Error as oracle_connection_exception:
                logger.log_error_msg(f'Oracle connection error, {oracle_connection_exception}', with_std_out=True)
                self.__discord_client.send_message(DiscordMessage(content='Database connection error'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG, with_text_to_speech=True)
            except Exception as exception:
                self.__discord_client.send_message(DiscordMessage(content='Fatal error'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
                stacktrace = traceback.format_exc()
                
                if len(stacktrace) > STACKTRACE_CHUNK_SIZE:
                    stacktrace_chunk_list = stacktrace.split('\n')
                    send_chunk_list = []

                    counter = 0
                    total_no_of_char = 0
                    concat_chunk_str = ''
                    while counter < len(stacktrace_chunk_list):
                        line_txt = stacktrace_chunk_list[counter]
                        line_txt_length = len(line_txt)
                        total_no_of_char += line_txt_length

                        if total_no_of_char <= STACKTRACE_CHUNK_SIZE:
                            concat_chunk_str += line_txt
                            counter += 1

                            if counter == len(stacktrace_chunk_list) - 1:
                                send_chunk_list.append(concat_chunk_str)
                        else:
                            send_chunk_list.append(concat_chunk_str)
                            total_no_of_char = 0
                            concat_chunk_str = ''

                    for send_chunk in send_chunk_list:
                        self.__discord_client.send_message(DiscordMessage(content=send_chunk), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)    
                else:
                    self.__discord_client.send_message(DiscordMessage(content=stacktrace), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)

                logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)
                logger.log_error_msg(f'{traceback.format_exc()}')
                logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_FATAL_ERROR_REFRESH_INTERVAL} seconds', with_std_out=True)
                time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)
                
 
    