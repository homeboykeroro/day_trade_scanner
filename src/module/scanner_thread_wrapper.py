import asyncio
import os
import threading
import time
import traceback
from typing import Callable
from aiohttp import ClientError
from requests import HTTPError, RequestException

from datasource.ib_connector import IBConnector

from sql.oracle_connector import OracleConnector

from module.discord_chatbot_client import DiscordChatBotClient

from model.discord.discord_message import DiscordMessage

from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

from exception.oracle_connection_error import OracleConnectionError

logger = Logger()

MAX_RETRY_CONNECTION_TIMES = 5
CONNECTION_FAIL_RETRY_INTERVAL = 10
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = 5

class ScannerThreadWrapper(threading.Thread):
    def __init__(self, scan: Callable, 
                 name: str,
                 discord_client: DiscordChatBotClient):
        self.exc = None
        self.__scan = scan
        self.__name = name
        self.__discord_client = discord_client
        super().__init__(name=name)
        
    def __reauthenticate(self, ib_connector: IBConnector):
        retry_times = 0
    
        while True:
            try:
                if retry_times < MAX_RETRY_CONNECTION_TIMES:
                    logger.log_debug_msg('send reauthenticate requests', with_std_out=True)
                    ib_connector.reauthenticate()
                else:
                    raise RequestException("Reauthentication failed")
            except Exception as reauthenticate_exception:
                if retry_times < MAX_RETRY_CONNECTION_TIMES:
                    self.__discord_client.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session, retry after {CONNECTION_FAIL_RETRY_INTERVAL} seconds')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                    retry_times += 1
                    time.sleep(CONNECTION_FAIL_RETRY_INTERVAL)
                    continue
                else:
                    self.__discord_client.send_message(DiscordMessage(content=f'Maximum re-authentication attemps exceed. Please restart application'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                    time.sleep(30)
                    os._exit(1)
            
            self.__discord_client.send_message_by_list_with_response([DiscordMessage(content='Reauthentication succeed')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            logger.log_debug_msg('Reauthentication succeed', with_std_out=True)
            break    
            
    def run(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            logger.log_debug_msg(f'Get event loop for {self.__name}')
        except RuntimeError:
            loop = asyncio.new_event_loop()
            logger.log_debug_msg(f'Create new event loop for {self.__name}')
            
        db_connector = OracleConnector()
        ib_connector = IBConnector(loop)
        
        while True:
            try:
                self.__scan(ib_connector, self.__discord_client, db_connector, loop)
            except (RequestException, ClientError, HTTPError) as connection_exception:
                self.__discord_client.send_message(DiscordMessage(content='Client Portal API connection failed, re-authenticating session'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                logger.log_error_msg(f'Client Portal API connection error in stock screener, {connection_exception}', with_std_out=True)
                self.__reauthenticate(ib_connector)
            except (OracleConnectionError) as oracle_connection_exception:
                self.__discord_client.send_message(DiscordMessage(content='Database connection error'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG, with_text_to_speech=True)
                logger.log_error_msg(f'Oracle connection error, {oracle_connection_exception}', with_std_out=True)
            except Exception as exception:
                self.__discord_client.send_message(DiscordMessage(content='Fatal error'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
                self.__discord_client.send_message(DiscordMessage(content=traceback.format_exc()), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
                logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)
                logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_FATAL_ERROR_REFRESH_INTERVAL} seconds', with_std_out=True)
                time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)
                