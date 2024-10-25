import threading

from constant.scanner.scanner_thread_name import ScannerThreadName

threading.current_thread().name = ScannerThreadName.SMALL_CAP_INITIAL_POP_SCANNER

import os
import time

from datasource.ib_connector import IBConnector

from scanner_wrapper import ScannerWrapper

from pattern.initial_pop import InitialPop

from module.discord_chatbot_client import DiscordChatBotClient

from utils.common.config_util import get_config
from utils.ib.filter_util import get_ib_scanner_filter
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.scanner.scanner_target import ScannerTarget
from constant.endpoint.ib.client_portal_api_endpoint import ClientPortalApiEndpoint
from constant.discord.discord_channel import DiscordChannel
from constant.candle.bar_size import BarSize

# Chatbot
small_cap_initial_pop_chatbot = DiscordChatBotClient()

ib_connector = IBConnector()
logger = Logger()

SCAN_PATTERN_NAME = 'SMALL_CAP_INITIAL_POP'
SCREENER_NAME = 'SMALL_CAP_TOP_GAINER_SCREENER'

# Chatbot Token
INITIAL_POP_CHATBOT_TOKEN = os.environ['DISCORD_SMALL_CAP_INITIAL_POP_CHATBOT_TOKEN']
CHATBOT_THREAD_NAME = 'small_cap_initial_pop_chatbot_thread'

# Log
SHOW_DISCORD_DEBUG_LOG = get_config(SCREENER_NAME, 'SHOW_DISCORD_DEBUG_LOG')

# Candlestick Chart Display Parameter
INITIAL_POP_DAILY_CANDLE_DAYS = get_config(SCAN_PATTERN_NAME, 'DAILY_CANDLE_DAYS')

# Analyser Criterion
MAX_TOLERANCE_PERIOD_IN_MINUTE = get_config(SCAN_PATTERN_NAME, 'MAX_TOLERANCE_PERIOD_IN_MINUTE')
MAX_POP_OCCURRENCE = get_config(SCAN_PATTERN_NAME, 'MAX_POP_OCCURRENCE')
MIN_GAP_UP_PCT = get_config(SCAN_PATTERN_NAME, 'MIN_GAP_UP_PCT')
MIN_CLOSE_PCT = get_config(SCAN_PATTERN_NAME, 'MIN_CLOSE_PCT')
DAILY_AND_MINUTE_CANDLE_GAP = get_config(SCAN_PATTERN_NAME, 'DAILY_AND_MINUTE_CANDLE_GAP')

# Top Gainer Filter Parameter
MAX_NO_OF_SCANNER_RESULT = get_config(SCREENER_NAME, 'MAX_NO_OF_SCANNER_RESULT')
MIN_PRICE = get_config(SCREENER_NAME, 'MIN_PRICE')
PERCENT_CHANGE_PARAM = get_config(SCREENER_NAME, 'PERCENT_CHANGE_PARAM')
MIN_USD_VOLUME = get_config(SCREENER_NAME, 'MIN_USD_VOLUME')
MAX_MARKET_CAP = get_config(SCREENER_NAME, 'MAX_MARKET_CAP')

# Scanner Idle Refresh Time
SCANNER_IDLE_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_IDLE_REFRESH_INTERVAL')

# API Endpoint Check Interval
SCANNER_API_ENDPOINT_CHECK_INTERVAL = get_config(SCREENER_NAME, 'SCANNER_API_ENDPOINT_CHECK_INTERVAL')
SNAPSHOT_API_ENDPOINT_CHECK_INTERVAL = get_config(SCREENER_NAME, 'SNAPSHOT_API_ENDPOINT_CHECK_INTERVAL')
MARKET_DATA_API_ENDPOINT_CHECK_INTERVAL = get_config(SCREENER_NAME, 'MARKET_DATA_API_ENDPOINT_CHECK_INTERVAL')

IB_TOP_GAINER_FILTER = get_ib_scanner_filter(scan_target=ScannerTarget.TOP_GAINER,
                                             min_price = MIN_PRICE, 
                                             percent_change_param = PERCENT_CHANGE_PARAM, 
                                             min_usd_volume = MIN_USD_VOLUME, 
                                             max_market_cap = MAX_MARKET_CAP, 
                                             additional_filter_list = [])

def scan():  
    small_cap_initial_pop_chatbot.run_chatbot(CHATBOT_THREAD_NAME, INITIAL_POP_CHATBOT_TOKEN)
           
    logger.log_debug_msg('Small cap initial pop scanner starts')
    small_cap_initial_pop_chatbot.send_message_by_list_with_response([DiscordMessage(content='Initial Pop Scanner Starts')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
    
    # Get contract list from IB screener
    ib_connector.acquire_api_endpoint_lock(ClientPortalApiEndpoint.RUN_SCANNER, SCANNER_API_ENDPOINT_CHECK_INTERVAL)
    logger.log_debug_msg(f'Fetch small cap top gainer screener result')
    contract_list = ib_connector.get_screener_results(MAX_NO_OF_SCANNER_RESULT, IB_TOP_GAINER_FILTER)
    
    if (ib_connector.check_if_contract_update_required(contract_list)):
        ib_connector.acquire_api_endpoint_lock(ClientPortalApiEndpoint.SNAPSHOT, SNAPSHOT_API_ENDPOINT_CHECK_INTERVAL)
        logger.log_debug_msg(f'Fetch small cap top gainer snapshot')
        ib_connector.update_contract_info(contract_list)
    
    ticker_to_contract_dict = ib_connector.get_ticker_to_contract_dict()
    
    ib_connector.acquire_api_endpoint_lock(ClientPortalApiEndpoint.MARKET_DATA_HISTORY, MARKET_DATA_API_ENDPOINT_CHECK_INTERVAL)
    one_minute_candle_df = ib_connector.retrieve_intra_day_minute_candle(contract_list=contract_list, bar_size=BarSize.ONE_MINUTE)
    
    ib_connector.acquire_api_endpoint_lock(ClientPortalApiEndpoint.MARKET_DATA_HISTORY, MARKET_DATA_API_ENDPOINT_CHECK_INTERVAL)
    daily_df = ib_connector.get_daily_candle(ib_connector=ib_connector,
                                             contract_list=contract_list, 
                                             offset_day=INITIAL_POP_DAILY_CANDLE_DAYS, 
                                             outside_rth=False)
            
    if SHOW_DISCORD_DEBUG_LOG:
        send_msg_start_time = time.time()
        small_cap_initial_pop_chatbot.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_GAINER_SCANNER_LIST)
        logger.log_debug_msg(f'Send top gainer scanner result time: {time.time() - send_msg_start_time}')

    small_cap_initial_pop_analyser = InitialPop(bar_size=BarSize.ONE_MINUTE,
                                                historical_data_df=one_minute_candle_df, 
                                                daily_df=daily_df, 
                                                ticker_to_contract_info_dict=ticker_to_contract_dict, 
                                                discord_client=small_cap_initial_pop_chatbot,
                                                min_gap_up_pct=MIN_GAP_UP_PCT,
                                                min_close_pct=MIN_CLOSE_PCT,
                                                max_pop_occurrence=MAX_POP_OCCURRENCE,
                                                max_tolerance_period_in_minute=MAX_TOLERANCE_PERIOD_IN_MINUTE,
                                                daily_and_minute_candle_gap=DAILY_AND_MINUTE_CANDLE_GAP,
                                                pattern_name=SCAN_PATTERN_NAME)
    small_cap_initial_pop_analyser.analyse()

def run():
    small_cap_initial_pop_scanner = ScannerWrapper(scanner_name='Small cap initial pop', 
                                                   scan=scan, 
                                                   discord_client=small_cap_initial_pop_chatbot)
    small_cap_initial_pop_scanner.run()

if __name__ == '__main__':
    run()