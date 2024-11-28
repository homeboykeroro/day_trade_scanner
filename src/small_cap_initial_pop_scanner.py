import time

from module.discord_chatbot import DiscordChatBot
from datasource.ib_connector import IBConnector

from pattern.initial_pop import InitialPop

from utils.common.config_util import get_config
from utils.ib.filter_util import get_ib_scanner_filter
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.scanner.scanner_target import ScannerTarget
from constant.discord.discord_message_channel import DiscordMessageChannel
from constant.candle.bar_size import BarSize

logger = Logger()

SCAN_PATTERN_NAME = 'SMALL_CAP_INITIAL_POP'
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
MAX_TOLERANCE_PERIOD_IN_MINUTE = get_config(SCAN_PATTERN_NAME, 'MAX_TOLERANCE_PERIOD_IN_MINUTE')
MAX_POP_OCCURRENCE = get_config(SCAN_PATTERN_NAME, 'MAX_POP_OCCURRENCE')
MIN_GAP_UP_PCT = get_config(SCAN_PATTERN_NAME, 'MIN_GAP_UP_PCT')
RAMP_UP_CANDLE_PCT = get_config(SCAN_PATTERN_NAME, 'RAMP_UP_CANDLE_PCT')
MIN_CLOSE_PCT = get_config(SCAN_PATTERN_NAME, 'MIN_CLOSE_PCT')
DAILY_AND_MINUTE_CANDLE_GAP = get_config(SCAN_PATTERN_NAME, 'DAILY_AND_MINUTE_CANDLE_GAP')
DAILY_CANDLE_DAYS = get_config(SCAN_PATTERN_NAME, 'DAILY_CANDLE_DAYS')

# API Endpoint Check Interval
SCANNER_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'SCANNER_API_ENDPOINT_LOCK_CHECK_INTERVAL')
SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL')
MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL')

# Log
SHOW_DISCORD_SCREENER_DEBUG_LOG = get_config(SCREENER_NAME, 'SHOW_DISCORD_DEBUG_LOG')

def small_cap_initial_pop_scan(ib_connector: IBConnector, discord_chatbot: DiscordChatBot):
    logger.log_debug_msg('Small cap initial pop scanner starts', with_std_out=True)
    
    if SHOW_DISCORD_SCREENER_DEBUG_LOG:
        send_msg_start_time = time.time()
        discord_chatbot.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordMessageChannel.SMALL_CAP_TOP_GAINER_SCREENER_LIST)
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
    
    small_cap_initial_pop_analyser = InitialPop(bar_size=BarSize.ONE_MINUTE,
                                                minute_candle_df=one_minute_candle_df, 
                                                daily_candle_df=daily_candle_df, 
                                                ticker_to_contract_info_dict=ticker_to_contract_dict, 
                                                discord_client=discord_chatbot,
                                                min_gap_up_pct=MIN_GAP_UP_PCT,
                                                ramp_up_candle_pct=RAMP_UP_CANDLE_PCT,
                                                min_close_pct=MIN_CLOSE_PCT,
                                                max_pop_occurrence=MAX_POP_OCCURRENCE,
                                                max_tolerance_period_in_minute=MAX_TOLERANCE_PERIOD_IN_MINUTE,
                                                daily_and_minute_candle_gap=DAILY_AND_MINUTE_CANDLE_GAP,
                                                pattern_name=SCAN_PATTERN_NAME,
                                                discord_channel=DiscordMessageChannel.SMALL_CAP_INITIAL_POP)
    small_cap_initial_pop_analyser.analyse()
