import threading

from constant.scanner.scanner_thread_name import ScannerThreadName

threading.current_thread().name = ScannerThreadName.YESTERDAY_TOP_GAINER.value

import os
import time
import datetime

from datasource.ib_connector import IBConnector

from scanner_wrapper import ScannerWrapper

from pattern.yesterday_bullish_daily_candle import YesterdayBullishDailyCandle

from module.discord_chatbot_client import DiscordChatBotClient
from module.yesterday_top_gainer_scraper import scrap

from utils.common.config_util import get_config
from utils.common.datetime_util import get_current_us_datetime, get_us_business_day
from utils.sql.previous_day_top_gainer_record_util import get_previous_day_top_gainer_list
from utils.sql.discord_message_record_util import check_if_pattern_analysis_message_sent
from utils.logger import Logger

from model.discord.discord_message import DiscordMessage

from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

# Chatbot
yesterday_top_gainer_chatbot = DiscordChatBotClient()

ib_connector = IBConnector()
logger = Logger()

SCAN_PATTERN_NAME = 'YESTERDAY_TOP_GAINER'

# Filter Criterion
MIN_CLOSE_PCT = get_config(SCAN_PATTERN_NAME, 'MIN_CLOSE_PCT')
DAILY_CANDLE_DAYS = get_config(SCAN_PATTERN_NAME, 'DAILY_CANDLE_DAYS')
MAX_OFFERING_NEWS_SIZE = get_config(SCAN_PATTERN_NAME, 'MAX_OFFERING_NEWS_SIZE')

# Refresh Time
REFRESH_INTERVAL = get_config(SCAN_PATTERN_NAME, 'REFRESH_INTERVAL')

# API Endpoint Check Interval
DEFAULT_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config('SYS_PARAM', 'DEFAULT_API_ENDPOINT_LOCK_CHECK_INTERVAL')
SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL')
MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config(SCAN_PATTERN_NAME, 'MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL')

date_to_filtered_top_gainer_list_dict = {}

def scan():
    logger.log_debug_msg('Yesterday top gainer scanner starts')
    start_time = time.time()
    
    us_current_datetime = get_current_us_datetime()
    day_offset = 0 if us_current_datetime.time() > datetime.time(16, 0, 0) else -1
    
    yesterday_top_gainer_retrieval_datetime = get_us_business_day(offset_day=day_offset, 
                                                                  us_date=us_current_datetime)

    yesterday_top_gainer_list = get_previous_day_top_gainer_list(pct_change=MIN_CLOSE_PCT, 
                                                                 start_datetime=yesterday_top_gainer_retrieval_datetime, 
                                                                 end_datetime=yesterday_top_gainer_retrieval_datetime)
    
    if not yesterday_top_gainer_list:
        scrap(yesterday_top_gainer_chatbot)
        yesterday_top_gainer_list = get_previous_day_top_gainer_list(pct_change=MIN_CLOSE_PCT, 
                                                                     start_datetime=yesterday_top_gainer_retrieval_datetime, 
                                                                     end_datetime=yesterday_top_gainer_retrieval_datetime)
    
    ticker_list = list(set([top_gainer[0] for top_gainer in yesterday_top_gainer_list]))
    logger.log_debug_msg(f'Retrieving yesterday top gainer ({yesterday_top_gainer_retrieval_datetime})', with_std_out=True)
    logger.log_debug_msg(f'Yesterday top gainer list size: {len(yesterday_top_gainer_list)}, top gainers: {yesterday_top_gainer_list}', with_std_out=True)

    new_yesterday_top_gainer_ticker_list = []

    filtered_yesterday_top_gainer_ticker_list = date_to_filtered_top_gainer_list_dict.get(yesterday_top_gainer_retrieval_datetime.date())
        
    # Check if message sent & meet filtered criterion
    if not filtered_yesterday_top_gainer_ticker_list:
        for ticker in ticker_list:
            is_yesterday_bullish_candle_analysis_msg_sent = check_if_pattern_analysis_message_sent(ticker=ticker, 
                                                                                                   hit_scanner_datetime=yesterday_top_gainer_retrieval_datetime.date(), 
                                                                                                   pattern=SCAN_PATTERN_NAME, 
                                                                                                   bar_size=BarSize.ONE_DAY.value)
            
            if not is_yesterday_bullish_candle_analysis_msg_sent:
                new_yesterday_top_gainer_ticker_list.append(ticker)
        
        logger.log_debug_msg(f'Analysing new top gainer: {new_yesterday_top_gainer_ticker_list}', with_std_out=True)
    else:
        # In case of failed message (not sent)
        for ticker in ticker_list:
            is_yesterday_bullish_candle_analysis_msg_sent = check_if_pattern_analysis_message_sent(ticker=ticker, 
                                                                                                   hit_scanner_datetime=yesterday_top_gainer_retrieval_datetime.date(), 
                                                                                                   pattern=SCAN_PATTERN_NAME, 
                                                                                                   bar_size=BarSize.ONE_DAY.value)
            meet_filter_criterion = ticker in filtered_yesterday_top_gainer_ticker_list
            if not is_yesterday_bullish_candle_analysis_msg_sent and meet_filter_criterion:
                new_yesterday_top_gainer_ticker_list.append(ticker)
        
        logger.log_debug_msg(f'Retry analysing top gainer: {new_yesterday_top_gainer_ticker_list}', with_std_out=True)

    if new_yesterday_top_gainer_ticker_list:
        yesterday_top_gainer_contract_list = ib_connector.fetch_contract_by_ticker_list(new_yesterday_top_gainer_ticker_list)
        ticker_to_contract_dict = ib_connector.fetch_snapshot(contract_list=yesterday_top_gainer_contract_list, 
                                                              snapshot_api_endpoint_lock_LOCK_check_interval=SNAPSHOT_API_ENDPOINT_LOCK_CHECK_INTERVAL)

        daily_candle_df = ib_connector.fetch_daily_candle(contract_list=yesterday_top_gainer_contract_list, 
                                                          offset_day=DAILY_CANDLE_DAYS, 
                                                          market_data_api_endpoint_lock_check_inverval=MARKET_DATA_API_ENDPOINT_LOCK_CHECK_INTERVAL)

        yesterday_bullish_daily_candle_analyser = YesterdayBullishDailyCandle(hit_scanner_date=yesterday_top_gainer_retrieval_datetime.date(),
                                                                              daily_df=daily_candle_df,
                                                                              ticker_to_contract_info_dict=ticker_to_contract_dict, 
                                                                              discord_client=yesterday_top_gainer_chatbot,
                                                                              min_close_pct=MIN_CLOSE_PCT,
                                                                              max_offering_news_size=MAX_OFFERING_NEWS_SIZE,
                                                                              pattern_name=SCAN_PATTERN_NAME)
        yesterday_bullish_daily_candle_analyser.analyse()
        date_to_filtered_top_gainer_list_dict[yesterday_top_gainer_retrieval_datetime.date()] = yesterday_bullish_daily_candle_analyser.filtered_ticker_list
        logger.log_debug_msg(f'List of yesterday top ganier with bullish daily candle: {date_to_filtered_top_gainer_list_dict[yesterday_top_gainer_retrieval_datetime.date()]}', with_std_out=True)
    
    logger.log_debug_msg(f'Yesterday top gainer scan time: {time.time() - start_time} seconds', with_std_out=True)
    time.sleep(REFRESH_INTERVAL)
            
def run():
    # Chatbot Token
    YESTERDAY_TOP_GAINER_SCANNER_CHATBOT_TOKEN = os.environ['DISCORD_YESTERDAY_TOP_GAINER_SCANNER_CHATBOT_TOKEN']
    CHATBOT_THREAD_NAME = 'yesterday_top_gainer_chatbot_thread'

    yesterday_top_gainer_chatbot.run_chatbot(CHATBOT_THREAD_NAME, YESTERDAY_TOP_GAINER_SCANNER_CHATBOT_TOKEN)
    yesterday_top_gainer_chatbot.send_message_by_list_with_response([DiscordMessage(content='Starts scanner')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
    
    yesterday_top_gainer_scanner = ScannerWrapper(scanner_name='Yesterday top gainer', 
                                                  scan=scan,
                                                  discord_client=yesterday_top_gainer_chatbot)
    yesterday_top_gainer_scanner.run()

if __name__ == '__main__':
    run()