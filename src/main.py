import os
import time
from aiohttp import ClientError
from requests import HTTPError, RequestException

from module.discord_chatbot_client import DiscordChatBotClient
from module.stock_screener import StockScreener
from module.pl_report_generator import PLReportGenerator

from datasource.ib_connector import IBConnector

from model.discord.discord_message import DiscordMessage

from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

MAX_RETRY_CONNECTION_TIMES = 5
CONNECTION_FAIL_RETRY_INTERVAL = 10

logger = Logger()

discord_client = DiscordChatBotClient()
ib_connector = IBConnector()
stock_screener = StockScreener(discord_client)
pl_report_generator = PLReportGenerator(discord_client)

def send_ib_preflight_request():
    try:
        ib_connector.check_auth_status()
        ib_connector.receive_brokerage_account()
    except Exception as preflight_request_exception:
        discord_client.send_message_by_list_with_response([DiscordMessage(content='Client Portal API preflight requests failed, re-authenticating seesion')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
        logger.log_error_msg(f'Client Portal API preflight requests error, {preflight_request_exception}', with_std_out=True)
        reauthenticate()

def reauthenticate():
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
                discord_client.send_message_by_list_with_response([DiscordMessage(content=f'Failed to re-authenticate session, retry after {CONNECTION_FAIL_RETRY_INTERVAL} seconds')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                logger.log_error_msg(f'Session re-authentication error, {reauthenticate_exception}', with_std_out=True)
                retry_times += 1
                time.sleep(CONNECTION_FAIL_RETRY_INTERVAL)
                continue
            else:
                discord_client.send_message(DiscordMessage(content=f'Maximum re-authentication attemps exceed. Please restart application'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                time.sleep(30)
                os._exit(1)
        discord_client.send_message_by_list_with_response([DiscordMessage(content='Reauthentication succeed')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
        logger.log_debug_msg('Reauthentication succeed', with_std_out=True)
        break    

def main():  
    try:
        discord_client.run_chatbot()

        while True:
            if discord_client.is_chatbot_ready:
                logger.log_debug_msg('Chatbot is ready', with_std_out=True)
                break
        
        send_ib_preflight_request()
        
        #pl_report_generator.run_pl_report()
        
        #https://www.geeksforgeeks.org/handling-a-threads-exception-in-the-caller-thread-in-python/
        while True:  # Add a loop to restart the thread if it fails
            try:
                logger.log_debug_msg('Create and start stock screener', with_std_out=True)
                discord_client.send_message_by_list_with_response([DiscordMessage(content='Stock screener started')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                stock_screener = StockScreener(discord_client)  # Recreate the thread
                stock_screener.start()
                stock_screener.join()
            except Exception as e:
                discord_client.send_message_by_list_with_response([DiscordMessage(content='Failed to start stock screener, reauthenticating')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                logger.log_error_msg(f'StockScreener thread error, {e}', with_std_out=True)
                reauthenticate()
                continue
            break
    except (RequestException, ClientError, HTTPError):
        reauthenticate()
 
if __name__ == '__main__':
    main()
