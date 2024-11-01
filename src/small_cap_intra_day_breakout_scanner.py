import threading

from constant.scanner.scanner_thread_name import ScannerThreadName

threading.current_thread().name = ScannerThreadName.INTRA_DAY_BREAKOUT.value

import os
import time

from datasource.ib_connector import IBConnector

from scanner_wrapper import ScannerWrapper

from pattern.intra_day_breakout import IntraDayBreakout

from module.discord_chatbot_client import DiscordChatBotClient

from utils.common.config_util import get_config
from utils.ib.filter_util import get_ib_scanner_filter
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.scanner.scanner_target import ScannerTarget
from constant.discord.discord_channel import DiscordChannel
from constant.candle.bar_size import BarSize

# Chatbot
small_cap_intra_day_breakout_chatbot = DiscordChatBotClient()

ib_connector = IBConnector()
logger = Logger()

SCAN_PATTERN_NAME = 'SMALL_CAP_INTRA_DAY_BREAKOUT'
SCREENER_NAME = 'SMALL_CAP_TOP_GAINER_SCREENER'

# Top Gainer Filter Parameter
MAX_NO_OF_SCANNER_RESULT = get_config(SCREENER_NAME, 'MAX_NO_OF_SCANNER_RESULT')
MIN_PRICE = get_config(SCREENER_NAME, 'MIN_PRICE')
PERCENT_CHANGE_PARAM = get_config(SCREENER_NAME, 'PERCENT_CHANGE_PARAM')
MIN_USD_VOLUME = get_config(SCREENER_NAME, 'MIN_USD_VOLUME')
MAX_MARKET_CAP = get_config(SCREENER_NAME, 'MAX_MARKET_CAP')

IB_TOP_GAINER_FILTER = get_ib_scanner_filter(scan_target=ScannerTarget.TOP_GAINER,
                                             min_price = MIN_PRICE, 
                                             percent_change_param = PERCENT_CHANGE_PARAM, 
                                             min_usd_volume = MIN_USD_VOLUME, 
                                             max_market_cap = MAX_MARKET_CAP, 
                                             additional_filter_list = [])

# Analyser Criterion
MIN_OBSERVE_PERIOD = get_config(SCAN_PATTERN_NAME, 'MIN_OBSERVE_PERIOD')
FIRST_POP_UP_MIN_CLOSE_PCT = get_config(SCAN_PATTERN_NAME, 'FIRST_POP_UP_MIN_CLOSE_PCT')
MIN_BREAKOUT_TRADING_VOLUME_IN_USD = get_config(SCAN_PATTERN_NAME, 'MIN_BREAKOUT_TRADING_VOLUME_IN_USD')
DAILY_CANDLE_DAYS = get_config(SCAN_PATTERN_NAME, 'DAILY_CANDLE_DAYS')

# API Endpoint Check Interval
SCANNER_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'SCANNER_API_ENDPOINT_LOCK_CHECK_INTERVAL')
SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL')
MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL')

# Log
SHOW_DISCORD_SCREENER_DEBUG_LOG = get_config(SCREENER_NAME, 'SHOW_DISCORD_DEBUG_LOG')

def scan():  
    logger.log_debug_msg('Small cap intra day breakout scanner starts')
    start_time = time.time()
    
    if SHOW_DISCORD_SCREENER_DEBUG_LOG:
        send_msg_start_time = time.time()
        small_cap_intra_day_breakout_chatbot.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_GAINER_SCANNER_LIST)
        logger.log_debug_msg(f'Send top gainer scanner result time: {time.time() - send_msg_start_time}')
    
    contract_list = ib_connector.fetch_screener_result(screener_filter=IB_TOP_GAINER_FILTER, 
                                                       max_no_of_scanner_result=MAX_NO_OF_SCANNER_RESULT, 
                                                       scanner_api_endpoint_lock_check_interval=SCANNER_API_ENDPOINT_LOCK_CHECK_INTERVAL)
    ticker_to_contract_dict = ib_connector.fetch_snapshot(contract_list=contract_list, 
                                                          snapshot_api_endpoint_lock_check_interval=SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL)
    one_minute_candle_df = ib_connector.fetch_intra_day_minute_candle(contract_list=contract_list, 
                                                                      market_data_api_endpoint_lock_check_interval=MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL)
    daily_candle_df = ib_connector.fetch_daily_candle(contract_list=contract_list, 
                                                      offset_day=DAILY_CANDLE_DAYS, 
                                                      market_data_api_endpoint_lock_check_inverval=MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL)
    
    intra_day_breakout_analyser = IntraDayBreakout(bar_size=BarSize.ONE_MINUTE,
                                                   minute_candle_df=one_minute_candle_df,
                                                   daily_df=daily_candle_df,
                                                   ticker_to_contract_info_dict=ticker_to_contract_dict,
                                                   discord_client=small_cap_intra_day_breakout_chatbot,
                                                   min_observe_period=MIN_OBSERVE_PERIOD,
                                                   first_pop_up_min_close_pct=FIRST_POP_UP_MIN_CLOSE_PCT,
                                                   min_breakout_trading_volume_in_usd=MIN_BREAKOUT_TRADING_VOLUME_IN_USD,
                                                   pattern_name=SCAN_PATTERN_NAME)
    intra_day_breakout_analyser.analyse()
    logger.log_debug_msg(f'Small cap intra day breakout scan time: {time.time() - start_time}', with_std_out=True)

def run():
    # Chatbot Token
    SMALL_CAP_INTRA_DAY_BREAKOUT_CHATBOT_TOKEN = os.environ['DISCORD_SMALL_CAP_INTRA_DAY_BREAKOUT_CHATBOT_TOKEN']
    CHATBOT_THREAD_NAME = 'small_cap_intra_day_breakout_chatbot_thread'

    small_cap_intra_day_breakout_chatbot.run_chatbot(CHATBOT_THREAD_NAME, SMALL_CAP_INTRA_DAY_BREAKOUT_CHATBOT_TOKEN)
    small_cap_intra_day_breakout_chatbot.send_message_by_list_with_response([DiscordMessage(content='Starts scanner')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
    
    small_cap_intra_day_breakout_scanner = ScannerWrapper(scanner_name='Small cap intra day breakout', 
                                                          scan=scan, 
                                                          discord_client=small_cap_intra_day_breakout_chatbot)
    small_cap_intra_day_breakout_scanner.run()
    
if __name__ == '__main__':
    run()