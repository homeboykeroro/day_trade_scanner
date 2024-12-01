import time
import threading
import traceback
from typing import Callable

from aiohttp import ClientError
from requests import HTTPError, RequestException

from module.discord_chatbot import DiscordChatBot

from model.discord.discord_message import DiscordMessage

from utils.common.string_util import split_long_paragraph_into_chunks
from utils.common.config_util import get_config
from utils.common.datetime_util import is_within_trading_day_and_hours
from utils.logger import Logger

from constant.discord.discord_message_channel import DiscordMessageChannel

logger = Logger()

# Scanner Idle Refresh Time
SCANNER_IDLE_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_IDLE_REFRESH_INTERVAL')

STACKTRACE_CHUNK_SIZE = get_config('SYS_PARAM', 'STACKTRACE_CHUNK_SIZE')
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_FATAL_ERROR_REFRESH_INTERVAL')

client_portal_connection_failed = False

class ScannerWrapper(threading.Thread):
    def __init__(self, scanner_name: str, scan: Callable, scan_parameter: dict, thread_name: str, discord_chatbot: DiscordChatBot):
        threading.Thread.__init__(self, name=thread_name)
        self.__scanner_name = scanner_name
        self.__scan = scan
        self.__scan_parameter = scan_parameter
        self.__discord_chatbot = discord_chatbot
        self.exc = None

    # https://www.geeksforgeeks.org/handling-a-threads-exception-in-the-caller-thread-in-python/
    def join(self):
        threading.Thread.join(self)

        if self.exc:
            raise self.exc

    def run(self):
        global client_portal_connection_failed
        self.exc = None 
        idle_message = None
        
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
                
        while True and not client_portal_connection_failed:
            try:
                start_time = time.time()
                self.__scan(**self.__scan_parameter)
                logger.log_debug_msg(f'{self.__scanner_name} scan time: {time.time() - start_time}', with_std_out=True)
            except (RequestException, ClientError, HTTPError) as connection_exception:
                client_portal_connection_failed = True
                self.exc = connection_exception
                break
            except Exception as e:
                self.exc = e
                
                self.__discord_chatbot.send_message_by_list_with_response([DiscordMessage(content='Fatal error')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
                stacktrace = traceback.format_exc()
                if len(stacktrace) > STACKTRACE_CHUNK_SIZE:
                    send_chunk_list = split_long_paragraph_into_chunks(stacktrace, STACKTRACE_CHUNK_SIZE)
                    for send_chunk in send_chunk_list:
                        self.__discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=send_chunk)], channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG)    
                else:
                    self.__discord_chatbot.send_message(DiscordMessage(content=stacktrace), channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG)
        
                logger.log_error_msg(f'Scanner fatal error, {e}', with_std_out=True)
                logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_FATAL_ERROR_REFRESH_INTERVAL} seconds', with_std_out=True)
                time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)
                
                #break
