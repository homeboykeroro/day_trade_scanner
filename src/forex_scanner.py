import time

from module.discord_chatbot import DiscordChatBot
from datasource.ib_connector import IBConnector

from pattern.forex_alert import ForexAlert

from constant.candle.bar_size import BarSize
from constant.discord.discord_message_channel import DiscordMessageChannel

from utils.sql.discord_message_record_util import check_if_pattern_analysis_message_sent_by_daily_basis
from utils.common.datetime_util import get_current_us_datetime
from utils.logger import Logger
from utils.common.config_util import get_config

SCAN_PATTERN_NAME = 'FOREX_ALERT'

logger = Logger()
ib_connector = IBConnector()

# Description    Con Id
# USD/HKD        12345777
# HKD/JPY        15016090
USD_HKD_CON_ID = get_config('FOREX', 'USD_HKD_CON_ID')
MIN_USD_HKD_TRIGGER_PRICE = get_config('FOREX', 'MIN_USD_HKD_TRIGGER_PRICE')
MAX_USD_HKD_TRIGGER_PRICE = get_config('FOREX', 'MAX_USD_HKD_TRIGGER_PRICE')
MAX_ALERT_TIMES = get_config('FOREX', 'MAX_ALERT_TIMES')
REFRESH_INTERVAL = get_config('FOREX', 'REFRESH_INTERVAL')

def forex_scan(ib_connector: IBConnector, discord_chatbot: DiscordChatBot):
    while True:
        start_time = time.time()
        logger.log_debug_msg('Forex scanner starts', with_std_out=True)
        
        is_message_send = check_if_pattern_analysis_message_sent_by_daily_basis(ticker='USD', 
                                                                                hit_scanner_datetime=get_current_us_datetime(), 
                                                                                pattern=SCAN_PATTERN_NAME, 
                                                                                bar_size=BarSize.ONE_MINUTE,
                                                                                max_occurrence=MAX_ALERT_TIMES)
        
        if not is_message_send:
            usd_one_minute_candle_df = ib_connector.fetch_intra_day_minute_candle(contract_list=[dict(con_id=USD_HKD_CON_ID, symbol='USD/HKD')])
            forex_analyser = ForexAlert(bar_size=BarSize.ONE_MINUTE, 
                                        minute_candle_df=usd_one_minute_candle_df,
                                        min_trigger_price=MIN_USD_HKD_TRIGGER_PRICE,
                                        max_trigger_price=MAX_USD_HKD_TRIGGER_PRICE,
                                        pattern_name=SCAN_PATTERN_NAME,
                                        discord_client=discord_chatbot,
                                        discord_channel=DiscordMessageChannel.FOREX)
            forex_analyser.analyse()
        
        logger.log_debug_msg(f'Forex analysis time: {time.time() - start_time} seconds')
        time.sleep(REFRESH_INTERVAL)
        
