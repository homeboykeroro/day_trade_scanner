import os
import time
import threading
import oracledb

from aiohttp import ClientError
from requests import HTTPError, RequestException

from scanner_wrapper import client_portal_connection_failed

from module.discord_chatbot import DiscordChatBot
from datasource.ib_connector import IBConnector
from scanner_wrapper import ScannerWrapper

from small_cap_initial_pop_scanner import small_cap_initial_pop_scan
from small_cap_intra_day_breakout_scanner import small_cap_intra_day_breakout_scan
from yesterday_top_gainer_scanner import yesterday_top_gainer_scan
from ipo_info_scraper import ipo_scan

from model.discord.discord_message import DiscordMessage

from constant.discord.discord_message_channel import DiscordMessageChannel
from constant.scanner.scanner_thread_name import ScannerThreadName

from utils.logger import Logger
from utils.common.config_util import get_config

ib_connector = IBConnector()
logger = Logger()

# Reauthentication Parameters
MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES = get_config('SYS_PARAM', 'MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES')
SCANNER_REAUTHENTICATION_RETRY_INTERVAL = get_config('SYS_PARAM', 'SCANNER_REAUTHENTICATION_RETRY_INTERVAL')
STACKTRACE_CHUNK_SIZE = get_config('SYS_PARAM', 'STACKTRACE_CHUNK_SIZE')
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_FATAL_ERROR_REFRESH_INTERVAL')

# Chatbot Token
CHATBOT_TOKEN = os.environ['DISCORD_CHATBOT_TOKEN']
CHATBOT_THREAD_NAME = 'main_chatbot_thread'
discord_chatbot = DiscordChatBot(CHATBOT_TOKEN)

current_reauthentication_retry_times = 0

def create_bot():
    global discord_chatbot
    discord_chatbot.run_chatbot()

bot_thread = threading.Thread(target=create_bot, name=CHATBOT_THREAD_NAME)
bot_thread.start()

while not discord_chatbot.is_chatbot_ready:
    continue

logger.log_debug_msg(f'Chatbot starts', with_std_out=True)

def reauthenticate():
    global client_portal_connection_failed
    global current_reauthentication_retry_times
    
    while True:
        try:
            if current_reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                logger.log_debug_msg('Send reauthenticate requests', with_std_out=True)
                ib_connector.reauthenticate()
            else:
                raise RequestException("Reauthentication failed")
        except (RequestException, ClientError, HTTPError) as reauthenticate_exception:
            if current_reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session IB client portal, retry after {SCANNER_REAUTHENTICATION_RETRY_INTERVAL} seconds')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                current_reauthentication_retry_times += 1
                time.sleep(SCANNER_REAUTHENTICATION_RETRY_INTERVAL)
                continue
            else:
                discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=f'Exceeds the maximum number of re-authentication retry times. Please restart scanner')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                time.sleep(30)
                os._exit(1)
        except Exception as exception:
            discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=f'IB client portal re-authentication fatal error. Please restart application')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            time.sleep(30)
            os._exit(1)
        
        try:
            ib_connector.check_auth_status()
            discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=f'IB client portal reauthentication succeed')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            logger.log_debug_msg(f'IB client portal re-authentication succeed', with_std_out=True)
            client_portal_connection_failed = False
            current_reauthentication_retry_times = 0
            break
        except Exception as auth_exception:
            logger.log_debug_msg(f'Re-authentication failed, {auth_exception}')
            discord_chatbot.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session IB client portal, retry after {SCANNER_REAUTHENTICATION_RETRY_INTERVAL} seconds')], channel_type=DiscordMessageChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            current_reauthentication_retry_times += 1
            time.sleep(SCANNER_REAUTHENTICATION_RETRY_INTERVAL)
            client_portal_connection_failed = True
            continue

def main():  
    scanner_list = []
    global current_reauthentication_retry_times
    
    def create_scanner():
        small_cap_initial_pop_scanner = ScannerWrapper(scanner_name='Small cap initial pop', 
                                               scan=small_cap_initial_pop_scan, 
                                               scan_parameter=dict(ib_connector=ib_connector, discord_chatbot=discord_chatbot),
                                               thread_name=ScannerThreadName.SMALL_CAP_INITIAL_POP_SCANNER.value,
                                               discord_chatbot=discord_chatbot)
        small_cap_intra_day_breakout_scanner = ScannerWrapper(scanner_name='Small cap intra day breakout', 
                                                              scan=small_cap_intra_day_breakout_scan, 
                                                              scan_parameter=dict(ib_connector=ib_connector, discord_chatbot=discord_chatbot),
                                                              thread_name=ScannerThreadName.SMALL_CAP_INTRA_DAY_BREAKOUT_SCANNER.value,
                                                              discord_chatbot=discord_chatbot)
        yesterday_top_gainer_scanner = ScannerWrapper(scanner_name='Yesterday top gainer bullish daily candle', 
                                                      scan=yesterday_top_gainer_scan, 
                                                      scan_parameter=dict(ib_connector=ib_connector, discord_chatbot=discord_chatbot),
                                                      thread_name=ScannerThreadName.YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_SCANNER.value,
                                                      discord_chatbot=discord_chatbot)
        ipo_scanner = ScannerWrapper(scanner_name='IPO list', 
                                     scan=ipo_scan, 
                                     scan_parameter=dict(discord_chatbot=discord_chatbot),
                                     thread_name=ScannerThreadName.IPO_INFO_SCRAPER.value,
                                     discord_chatbot=discord_chatbot)
        
        scanner_list = [
            small_cap_initial_pop_scanner,
            small_cap_intra_day_breakout_scanner,
            yesterday_top_gainer_scanner,
            ipo_scanner
        ]
        
        return scanner_list
        
    scanner_list = create_scanner()
    
    try:
        ib_connector.receive_brokerage_account()
    except HTTPError as preflight_request_exception:
        logger.log_error_msg(f'Client portal API preflight request error, {preflight_request_exception}', with_std_out=True)
        reauthenticate()
        
    while not client_portal_connection_failed:
        for scanner in scanner_list:
            scanner.start()
        
        try:
            for scanner in scanner_list:
                scanner.join()
        except (RequestException, ClientError, HTTPError) as connection_exception:
            # if any of these threads catch RequestException, ClientError, HTTPError
            logger.log_error_msg(f'Client portal API connection error, {connection_exception}', with_std_out=True)
            reauthenticate(ib_connector, current_reauthentication_retry_times)
            create_scanner()
        except oracledb.Error as oracle_connection_exception:
            logger.log_error_msg(f'Oracle connection error, {oracle_connection_exception}', with_std_out=True)
            discord_chatbot.send_message(DiscordMessage(content=f'Database connection error, {oracle_connection_exception}'), channel_type=DiscordMessageChannel.CHATBOT_ERROR_LOG, with_text_to_speech=True)
            time.sleep(30)
            os._exit(1)

if __name__ == '__main__':
    main()