import threading

import os
import time
import oracledb
from aiohttp import ClientError
from requests import HTTPError, RequestException
import traceback
from typing import Callable

from datasource.ib_connector import IBConnector

from module.discord_chatbot_client import DiscordChatBotClient

from utils.common.string_util import split_long_paragraph_into_chunks
from utils.common.config_util import get_config
from utils.common.datetime_util import is_within_trading_day_and_hours
from utils.sql.api_endpoint_lock_record_util import get_locked_api_endpoint, update_api_endpoint_lock
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.discord.discord_channel import DiscordChannel

ib_connector = IBConnector()
logger = Logger()

# Scanner Idle Refresh Time
SCANNER_IDLE_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_IDLE_REFRESH_INTERVAL')

# Reauthentication Parameters
MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES = get_config('SYS_PARAM', 'MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES')
SCANNER_REAUTHENTICATION_RETRY_INTERVAL = get_config('SYS_PARAM', 'SCANNER_REAUTHENTICATION_RETRY_INTERVAL')
STACKTRACE_CHUNK_SIZE = get_config('SYS_PARAM', 'STACKTRACE_CHUNK_SIZE')
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_FATAL_ERROR_REFRESH_INTERVAL')

class ScannerWrapper():
    def __init__(self, scanner_name: str, scan: Callable, discord_client: DiscordChatBotClient):
        self.__scanner_name = scanner_name
        self.__scan = scan
        self.__discord_client = discord_client

    def release_all_api_endpoint_lock(self):
        try:
            thread_name = threading.current_thread().name
            locked_api_endpoint_list = get_locked_api_endpoint(locked_by=thread_name)
            logger.log_debug_msg(f'Releasing API endpoint locked by {thread_name}, locked API endpoint list: {locked_api_endpoint_list}')
            release_api_endpoint_list = [dict(endpoint=endpoint, locked_by=None, is_locked='N', lock_datetime=None) for endpoint in locked_api_endpoint_list]
            
            if release_api_endpoint_list:
                update_api_endpoint_lock(release_api_endpoint_list)
        except Exception as e:
            logger.log_error_msg(f'API endpoint release error, {e}', with_std_out=True)
            self.__discord_client.send_message(DiscordMessage(content=f'Database connection error, please restart'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            self.__discord_client.send_message(DiscordMessage(content=f'Database connection error, {e}'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG, with_text_to_speech=False)
            time.sleep(30)
            os._exit(1)

    def reauthenticate(self, ib_connector: IBConnector, reauthentication_retry_times: int):
        while True:
            try:
                if reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                    logger.log_debug_msg('Send reauthenticate requests', with_std_out=True)
                    ib_connector.reauthenticate()
                else:
                    raise RequestException("Reauthentication failed")
            except (RequestException, ClientError, HTTPError)  as reauthenticate_exception:
                if reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                    self.__discord_client.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session for {self.__scanner_name} scanner, retry after {SCANNER_REAUTHENTICATION_RETRY_INTERVAL} seconds')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                    reauthentication_retry_times += 1
                    time.sleep(SCANNER_REAUTHENTICATION_RETRY_INTERVAL)
                    continue
                else:
                    self.__discord_client.send_message(DiscordMessage(content=f'Exceeds the maximum number of re-authentication retry times. Please restart {self.__scanner_name} scanner'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    time.sleep(30)
                    os._exit(1)
            except Exception as exception:
                self.__discord_client.send_message(DiscordMessage(content=f'{self.__scanner_name} scanner re-authentication fatal error. Please restart {self.__scanner_name} scanner'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                time.sleep(30)
                os._exit(1)

            self.__discord_client.send_message_by_list_with_response([DiscordMessage(content=f'{self.__scanner_name} scanner reauthentication succeed')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            logger.log_debug_msg(f'{self.__scanner_name} scanner reauthentication succeed', with_std_out=True)
            break 
        
    def run(self):
        idle_message = None
        reauthentication_retry_times = 0

        while True: 
            start_scan = is_within_trading_day_and_hours()

            if start_scan:
                break
            else:
                if idle_message:
                    logger.log_debug_msg(idle_message, with_std_out=True)
                else:
                    idle_message = f'{threading.current_thread().name} is idle until valid trading weekday and time'

                time.sleep(SCANNER_IDLE_REFRESH_INTERVAL)
                
        while True:    
            try:
                scan_start_time = time.time()
                self.__scan()
                reauthentication_retry_times = 0
                logger.log_debug_msg(f'{self.__scanner_name} scan completed, completed in {time.time() - scan_start_time}')
            except (RequestException, ClientError, HTTPError) as connection_exception:
                self.release_all_api_endpoint_lock()
                
                logger.log_error_msg(f'Client portal API connection error, {connection_exception}', with_std_out=True)
                self.reauthenticate(ib_connector, reauthentication_retry_times)
            except oracledb.Error as oracle_connection_exception:
                logger.log_error_msg(f'Oracle connection error, {oracle_connection_exception}', with_std_out=True)
                self.__discord_client.send_message(DiscordMessage(content=f'Database connection error, {oracle_connection_exception}'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG, with_text_to_speech=True)
                time.sleep(30)
                os._exit(1)
            except Exception as exception:
                self.release_all_api_endpoint_lock()
                
                self.__discord_client.send_message_by_list_with_response([DiscordMessage(content='Fatal error')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
                stacktrace = traceback.format_exc()

                if len(stacktrace) > STACKTRACE_CHUNK_SIZE:
                    send_chunk_list = split_long_paragraph_into_chunks(stacktrace, STACKTRACE_CHUNK_SIZE)

                    for send_chunk in send_chunk_list:
                        self.__discord_client.send_message_by_list_with_response([DiscordMessage(content=send_chunk)], channel_type=DiscordChannel.CHATBOT_ERROR_LOG)    
                else:
                    self.__discord_client.send_message(DiscordMessage(content=stacktrace), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)

                logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)
                logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_FATAL_ERROR_REFRESH_INTERVAL} seconds', with_std_out=True)
                time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)
