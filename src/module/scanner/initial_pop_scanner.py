import os
import time
import oracledb
from aiohttp import ClientError
import requests
from requests import HTTPError, RequestException
import traceback

from datasource.ib_connector import IBConnector

from module.discord_chatbot_client import DiscordChatBotClient

from utils.config_util import get_config
from utils.filter_util import get_ib_scanner_filter
from utils.datetime_util import is_within_trading_day_and_hours
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.scanner.scanner_target import ScannerTarget
from constant.endpoint.ib.client_portal_api_endpoint import ClientPortalApiEndpoint
from constant.discord.discord_channel import DiscordChannel



discord_client = DiscordChatBotClient()
ib_connector = IBConnector()
logger = Logger()

# Chatbot Token
CHATBOT_TOKEN = os.environ['DISCORD_CHATBOT_TOKEN']

# Top gainer filter parameter
MAX_NO_OF_DAY_TRADE_SCANNER_RESULT = get_config('TOP_GAINER_SCANNER', 'MAX_NO_OF_DAY_TRADE_SCANNER_RESULT')
MIN_PRICE = get_config('TOP_GAINER_SCANNER', 'MIN_PRICE')
PERCENT_CHANGE_PARAM = get_config('TOP_GAINER_SCANNER', 'PERCENT_CHANGE_PARAM')
MIN_USD_VOLUME = get_config('TOP_GAINER_SCANNER', 'MIN_USD_VOLUME')
MAX_MARKET_CAP = get_config('TOP_GAINER_SCANNER', 'MAX_MARKET_CAP')

# Scanner Idle Refresh Time
SCANNER_IDLE_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_IDLE_REFRESH_INTERVAL')

# Reeauthentication Parameters
MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES = get_config('SYS_PARAM', 'MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES')
SCANNER_REAUTHENTICATION_RETRY_INTERVAL = get_config('SYS_PARAM', 'SCANNER_REAUTHENTICATION_RETRY_INTERVAL')
STACKTRACE_CHUNK_SIZE = get_config('SYS_PARAM', 'STACKTRACE_CHUNK_SIZE')
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_FATAL_ERROR_REFRESH_INTERVAL')

IB_TOP_GAINER_FILTER = get_ib_scanner_filter(scan_target=ScannerTarget.TOP_GAINER,
                                             min_price = MIN_PRICE, 
                                             percent_change_param = PERCENT_CHANGE_PARAM, 
                                             min_usd_volume = MIN_USD_VOLUME, 
                                             max_market_cap = MAX_MARKET_CAP, 
                                             additional_filter_list = [])

def reauthenticate(ib_connector: IBConnector, discord_client: DiscordChatBotClient, reauthentication_retry_times: int):
    while True:
        try:
            if reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                logger.log_debug_msg('Send reauthenticate requests', with_std_out=True)
                ib_connector.reauthenticate()
            else:
                raise RequestException("Reauthentication failed")
        except (RequestException, ClientError, HTTPError)  as reauthenticate_exception:
            if reauthentication_retry_times < MAX_REAUTHENTICATION_RETRY_CONNECTION_TIMES:
                discord_client.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session for initial pop scanner, retry after {SCANNER_REAUTHENTICATION_RETRY_INTERVAL} seconds')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                reauthentication_retry_times += 1
                time.sleep(SCANNER_REAUTHENTICATION_RETRY_INTERVAL)
                continue
            else:
                discord_client.send_message(DiscordMessage(content=f'Exceeds the maximum number of re-authentication retry times. Please restart inital pop scanner'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                time.sleep(30)
                os._exit(1)
        except Exception as exception:
            discord_client.send_message(DiscordMessage(content=f'Initial pop scanner re-authentication fatal error. Please restart initial pop scanner'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            time.sleep(30)
            os._exit(1)

        discord_client.send_message_by_list_with_response([DiscordMessage(content='Initial pop scanner reauthentication succeed')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
        logger.log_debug_msg('Reauthentication succeed', with_std_out=True)
        break 

def scan():  
    idle_message = None
    reauthentication_retry_times = 0
    discord_client.run_chatbot(CHATBOT_TOKEN)
    
    while True: 
        start_scan = is_within_trading_day_and_hours()
            
        if start_scan:
            break
        else:
            if idle_message:
                logger.log_debug_msg(idle_message, with_std_out=True)
            else:
                idle_message = 'Scanner is idle until valid trading weekday and time'

            time.sleep(SCANNER_IDLE_REFRESH_INTERVAL)
            
    logger.log_debug_msg('Starts Initial Pop Scanner', with_std_out=True)
    discord_client.send_message_by_list_with_response([DiscordMessage(content='Initial Pop Scanner Starts')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)

    # Sending pre-flight request
    try:
        ib_connector.receive_brokerage_account()
    except requests.exceptions.HTTPError as preflight_request_exception:
        logger.log_error_msg(f'Client Portal API preflight requests error, {preflight_request_exception}', with_std_out=True)
        reauthenticate(ib_connector, discord_client, reauthentication_retry_times)
        
    reauthentication_retry_times = 0

    # Keep scanning
    while True:    
        try:
            ib_connector.control_api_endpoint_rate_limit(ClientPortalApiEndpoint.RUN_SCANNER, 1)
            contract_list = ib_connector.get_screener_results(MAX_NO_OF_DAY_TRADE_SCANNER_RESULT, IB_TOP_GAINER_FILTER)
            
            
            
            reauthentication_retry_times = 0
        except (RequestException, ClientError, HTTPError) as connection_exception:
            logger.log_error_msg(f'Client portal API connection error, {connection_exception}', with_std_out=True)
            reauthenticate(ib_connector, discord_client, reauthentication_retry_times)
        except oracledb.Error as oracle_connection_exception:
            logger.log_error_msg(f'Oracle connection error, {oracle_connection_exception}', with_std_out=True)
            discord_client.send_message(DiscordMessage(content='Database connection error'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG, with_text_to_speech=True)
        except Exception as exception:
            discord_client.send_message(DiscordMessage(content='Fatal error'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
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
                    discord_client.send_message_by_list_with_response([DiscordMessage(content=send_chunk)], channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
            else:
                discord_client.send_message(DiscordMessage(content=stacktrace), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)

                logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)
                logger.log_error_msg(f'{traceback.format_exc()}')
                logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_FATAL_ERROR_REFRESH_INTERVAL} seconds', with_std_out=True)
                time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)

if __name__ == '__main__':
    scan()