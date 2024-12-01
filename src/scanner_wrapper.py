import threading
import time
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
    def __init__(self, scanner_name: str, 
                       scan: Callable, 
                       scan_parameter: dict, 
                       thread_name: str, 
                       discord_chatbot: DiscordChatBot):
        threading.Thread.__init__(self, name=thread_name)
        
        self.__scanner_name = scanner_name
        self.__scan = scan
        self.__scan_parameter = scan_parameter
        self.__thread_name = thread_name
        self.__discord_chatbot = discord_chatbot
        self.exc = None

    def run(self):
        threading.current_thread().name = self.__thread_name
        
        global client_portal_connection_failed
        self.exc = None
        
        idle_message_shown = False
        
        # while True: 
        #     start_scan = is_within_trading_day_and_hours()

        #     if start_scan:
        #         break
        #     else:
        #         if not idle_message_shown:
        #             logger.log_debug_msg(f'{threading.current_thread().name} is idle until valid trading weekday and time', with_std_out=True)
        #             idle_message_shown = True

        #         time.sleep(SCANNER_IDLE_REFRESH_INTERVAL)
            
        try:
            logger.log_debug_msg(f'Thread {self.name} starting run', with_std_out=True)
            while not client_portal_connection_failed:
                start_time = time.time()
                logger.log_debug_msg(f'Initalising {threading.current_thread().name}', with_std_out=True)
                self.__scan(**self.__scan_parameter)
                logger.log_debug_msg(f'{self.__scanner_name} scan time: {time.time() - start_time}', with_std_out=True)
        except (RequestException, ClientError, HTTPError) as connection_exception:
            client_portal_connection_failed = True
            self.exc = connection_exception
            logger.log_error_msg(f'Thread {self.name} caught exception: {connection_exception}', with_std_out=True)
        except Exception as e:
            self.exc = e
            stacktrace = traceback.format_exc()
            logger.log_error_msg(f'Thread {self.name} caught exception: {stacktrace}', with_std_out=True)
            self.__discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=f' {self.__scanner_name} scanner fatal error')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            if len(stacktrace) > STACKTRACE_CHUNK_SIZE:
                send_chunk_list = split_long_paragraph_into_chunks(stacktrace, STACKTRACE_CHUNK_SIZE)
                for send_chunk in send_chunk_list:
                    self.__discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=send_chunk)], channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG)
            else:
                self.__discord_chatbot.send_message(DiscordMessage(content=stacktrace), channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG)
            time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)
        finally:
            logger.log_debug_msg(f'Thread {self.name} finished execution', with_std_out=True)
