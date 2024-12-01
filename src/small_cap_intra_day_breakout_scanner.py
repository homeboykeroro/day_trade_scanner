import time

from module.discord_chatbot import DiscordChatBot
from datasource.ib_connector import IBConnector

from pattern.intra_day_breakout import IntraDayBreakout

from utils.common.config_util import get_config
from utils.ib.filter_util import get_ib_scanner_filter
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.scanner.scanner_target import ScannerTarget
from constant.discord.discord_message_channel import DiscordMessageChannel
from constant.candle.bar_size import BarSize

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
DAILY_AND_MINUTE_CANDLE_GAP = get_config(SCAN_PATTERN_NAME, 'DAILY_AND_MINUTE_CANDLE_GAP')

# Log
SHOW_DISCORD_SCREENER_DEBUG_LOG = get_config(SCREENER_NAME, 'SHOW_DISCORD_DEBUG_LOG')

def small_cap_intra_day_breakout_scan(ib_connector: IBConnector, discord_chatbot: DiscordChatBot):
    logger.log_debug_msg('Small cap intra day breakout scanner starts', with_std_out=True)
    start_time = time.time()
    
    if SHOW_DISCORD_SCREENER_DEBUG_LOG:
        send_msg_start_time = time.time()
        discord_chatbot.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordMessageChannel.SMALL_CAP_TOP_GAINER_SCREENER_LIST)
        logger.log_debug_msg(f'Send top gainer scanner result time: {time.time() - send_msg_start_time}')
    
    contract_list = ib_connector.fetch_screener_result(screener_filter=IB_TOP_GAINER_FILTER, 
                                                       max_no_of_scanner_result=MAX_NO_OF_SCANNER_RESULT)
    ticker_to_contract_dict = ib_connector.fetch_snapshot(contract_list=contract_list)
    one_minute_candle_df = ib_connector.fetch_intra_day_minute_candle(contract_list=contract_list)
    daily_candle_df = ib_connector.fetch_daily_candle(contract_list=contract_list, 
                                                      offset_day=DAILY_CANDLE_DAYS)
    
    intra_day_breakout_analyser = IntraDayBreakout(bar_size=BarSize.ONE_MINUTE,
                                                   minute_candle_df=one_minute_candle_df,
                                                   daily_candle_df=daily_candle_df,
                                                   ticker_to_contract_info_dict=ticker_to_contract_dict,
                                                   discord_client=discord_chatbot,
                                                   min_observe_period=MIN_OBSERVE_PERIOD,
                                                   first_pop_up_min_close_pct=FIRST_POP_UP_MIN_CLOSE_PCT,
                                                   min_breakout_trading_volume_in_usd=MIN_BREAKOUT_TRADING_VOLUME_IN_USD,
                                                   daily_and_minute_candle_gap=DAILY_AND_MINUTE_CANDLE_GAP,
                                                   pattern_name=SCAN_PATTERN_NAME)
    intra_day_breakout_analyser.analyse()
    logger.log_debug_msg(f'Small cap intra day breakout scan time: {time.time() - start_time}', with_std_out=True)