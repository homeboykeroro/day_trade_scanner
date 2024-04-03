import math
import datetime
import pandas as pd 

from module.discord_chatbot_client import DiscordChatBotClient

from datasource.ib_connector import IBConnector
from sql.sqlite_connector import SqliteConnector

from pattern.initial_pop import InitialPop
from pattern.initial_dip import InitialDip
from pattern.yesterday_bullish_daily_candle import YesterdayBullishDailyCandle

from model.discord.discord_message import DiscordMessage

from utils.previous_day_top_gainer_util import get_previous_day_top_gainer_list
from utils.filter_util import get_ib_scanner_filter
from utils.nasdaq_data_util import get_all_ticker_in_the_market
from utils.datetime_util import PRE_MARKET_START_DATETIME, get_current_us_datetime, get_us_business_day
from utils.dataframe_util import append_customised_indicator
from utils.logger import Logger

from constant.scanner.scanner_target import ScannerTarget
from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

idx = pd.IndexSlice
logger = Logger()

# Day Trade
MAX_MARKET_CAP_FOR_DAY_TRADE_SCANNER = 1e6 * 500
MIN_PRICE_FOR_DAY_TRADE_SCANNER = 0.3
MAX_NO_OF_DAY_TRADE_SCANNER_RESULT = 15
IB_TOP_GAINER_FILTER = get_ib_scanner_filter(ScannerTarget.TOP_GAINER,
                                       min_price = MIN_PRICE_FOR_DAY_TRADE_SCANNER, 
                                       percent_change_param = 10, 
                                       min_usd_volume = 20000, 
                                       max_market_cap = MAX_MARKET_CAP_FOR_DAY_TRADE_SCANNER, 
                                       additional_filter_list = [])
IB_TOP_LOSER_FILTER = get_ib_scanner_filter(ScannerTarget.TOP_LOSER,
                                      min_price = MIN_PRICE_FOR_DAY_TRADE_SCANNER, 
                                      percent_change_param = -10, 
                                      min_usd_volume = 20000, 
                                      max_market_cap = MAX_MARKET_CAP_FOR_DAY_TRADE_SCANNER, 
                                      additional_filter_list = [])

# Swing Trade
PREVIOUS_DAY_TOP_GAINER_MIN_PCT_CHANGE = 30
PREVIOUS_DAY_TOP_GAINER_MAX_OBSERVE_DAYS = 5

#IB_CLOSEST_TO_HALT_FILTER = get_ib_scanner_filter(ScanCode)

class Scanner:
    def __init__(self, discord_client: DiscordChatBotClient, ib_connector: IBConnector, sqlite_connector: SqliteConnector) -> None:
        self.__discord_client = discord_client
        self.__ib_connector = ib_connector
        self.__sqllite_connector = sqlite_connector
        
        self.__all_ticker_list = get_all_ticker_in_the_market()
        self.__daily_canlde_df = pd.DataFrame()
        
        self.__yesterday_top_gainier_minute_candle_df_dict = {
            BarSize.ONE_MINUTE: pd.DataFrame()
        }
        
    def scan_yesterday_top_gainer(self):
        yesterday_top_gainer_contract_list = self.__get_previous_day_top_gainers_contracts(offset=-1) 
        previous_day_top_gainers_df = self.__get_daily_candle(contract_list=yesterday_top_gainer_contract_list, 
                                                              offset_day=PREVIOUS_DAY_TOP_GAINER_MAX_OBSERVE_DAYS,
                                                              outside_rth=False)
        
        with pd.option_context('display.max_rows', None,
                   'display.max_columns', None,
                   'display.precision', 3):
            logger.log_debug_msg(f'previous day top gainer df: {previous_day_top_gainers_df}', with_log_file=True, with_std_out=False)
          
        yesterday_bullish_daily_candle_analyser = YesterdayBullishDailyCandle(daily_df=previous_day_top_gainers_df,
                                                                              ticker_to_contract_info_dict=self.__ib_connector.get_ticker_to_contract_dict(), 
                                                                              discord_client=self.__discord_client, 
                                                                              sqlite_connector=self.__sqllite_connector)
        yesterday_bullish_daily_candle_analyser.analyse()
        
        intra_day_one_minute_candle_df = self.__retrieve_intra_day_minute_candle(yesterday_top_gainer_contract_list, 
                                                                                 BarSize.ONE_MINUTE)
        yesterday_one_minute_candle_df = self.__retrieve_yesterday_minute_candle(yesterday_top_gainer_contract_list, 
                                                                                 BarSize.ONE_MINUTE)
        concated_one_minute_candle_df = pd.concat([yesterday_one_minute_candle_df,
                                                  intra_day_one_minute_candle_df], axis=0)
        
        with pd.option_context('display.max_rows', None,
                   'display.max_columns', None,
                   'display.precision', 3):
            logger.log_debug_msg(f'yesterday top gainer\'s intra_day_one_minute_candle_df: {intra_day_one_minute_candle_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'yesterday top gainer\'s yesterday_one_minute_candle_df: {yesterday_one_minute_candle_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'yesterday top gainer\'s concated_one_minute_candle_df: {concated_one_minute_candle_df}', with_log_file=True, with_std_out=False)
    
    def scan_top_gainer(self) -> None:
        contract_list = self.__ib_connector.get_screener_results(MAX_NO_OF_DAY_TRADE_SCANNER_RESULT, IB_TOP_GAINER_FILTER)
        self.__ib_connector.update_contract_info(contract_list)
        one_minute_candle_df = self.__retrieve_intra_day_minute_candle(contract_list, BarSize.ONE_MINUTE)
        daily_df = self.__get_daily_candle(contract_list=contract_list, 
                                           offset_day=5, 
                                           outside_rth=False)

        logger.log_debug_msg(f'top gainer scanner result: {[contract["symbol"] for contract in contract_list]}')
        self.__discord_client.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_GAINER_SCANNER_LIST)
        
        initial_pop_analyser = InitialPop(bar_size=BarSize.ONE_MINUTE,
                                          historical_data_df=one_minute_candle_df, 
                                          daily_df=daily_df, 
                                          ticker_to_contract_info_dict=self.__ib_connector.get_ticker_to_contract_dict(), 
                                          discord_client=self.__discord_client,
                                          sqlite_connector=self.__sqllite_connector)
        initial_pop_analyser.analyse()
            
    def scan_top_loser(self) -> None:
        contract_list = self.__ib_connector.get_screener_results(MAX_NO_OF_DAY_TRADE_SCANNER_RESULT, IB_TOP_LOSER_FILTER)
        self.__ib_connector.update_contract_info(contract_list)
        one_minute_candle_df = self.__retrieve_intra_day_minute_candle(contract_list, BarSize.ONE_MINUTE)
        daily_df = self.__get_daily_candle(contract_list=contract_list, 
                                           offset_day=5, 
                                           outside_rth=False)
        
        logger.log_debug_msg(f'top loser scanner result: {[contract["symbol"] for contract in contract_list]}')
        self.__discord_client.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_LOSER_SCANNER_LIST)
        
        initial_dip_analyser = InitialDip(bar_size=BarSize.ONE_MINUTE,
                                          historical_data_df=one_minute_candle_df, 
                                          daily_df=daily_df, 
                                          ticker_to_contract_info_dict=self.__ib_connector.get_ticker_to_contract_dict(), 
                                          discord_client=self.__discord_client,
                                          sqlite_connector=self.__sqllite_connector)
        initial_dip_analyser.analyse()
    
    def scan_closest_to_halt(self):
        pass
    
    def scan_halted(self):
        pass

    def __retrieve_intra_day_minute_candle(self, contract_list: list, bar_size: BarSize) -> pd.DataFrame:
        us_current_datetime = get_current_us_datetime().replace(microsecond=0, second=0)
        historical_data_interval_in_minute = (us_current_datetime - PRE_MARKET_START_DATETIME).total_seconds() / 60
        logger.log_debug_msg(f'Historical candle data retrieval period: {historical_data_interval_in_minute} minutes')
    
        if historical_data_interval_in_minute < 1:
            logger.log_debug_msg('Historical candle data retrieval retrieval period is less than 1 minute', with_std_out=True)
            return None
        else:
            candle_df = self.__ib_connector.get_historical_candle_df(contract_list=contract_list, 
                                                                     period=f'{math.floor(historical_data_interval_in_minute)}min', 
                                                                     bar_size=bar_size, 
                                                                     outside_rth='true')
            return append_customised_indicator(candle_df)
    
    def __retrieve_yesterday_minute_candle(self, contract_list: list, bar_size: BarSize, outside_rth: bool = True):
        candle_request_contract_list = []
        contract_ticker_list = [contract['symbol'] for contract in contract_list]
        minute_candle_df = self.__yesterday_top_gainier_minute_candle_df_dict[bar_size]
        yesterday_top_gainer_ticker_list = list(minute_candle_df.columns.get_level_values(0).unique())
        
        for contract in contract_list:
            if contract['symbol'] not in yesterday_top_gainer_ticker_list:
                candle_request_contract_list.append(contract)
                
        if candle_request_contract_list:
            candle_retrieval_end_datetime = get_us_business_day(offset_day=-1)
            
            if outside_rth:
                outside_rth_str = 'true'
                candle_retrieval_end_datetime = candle_retrieval_end_datetime.replace(hour=20, minute=0, second=0, microsecond=0)
            else:
                outside_rth_str = 'false'
                candle_retrieval_end_datetime = candle_retrieval_end_datetime.replace(hour=16, minute=0, second=0, microsecond=0)

            candle_df = self.__ib_connector.get_historical_candle_df(contract_list=candle_request_contract_list, 
                                                                     period='960min', 
                                                                     bar_size=bar_size, 
                                                                     outside_rth=outside_rth_str, 
                                                                     candle_retrieval_end_datetime=candle_retrieval_end_datetime)
            complete_df = append_customised_indicator(candle_df)
            
            self.__yesterday_top_gainier_minute_candle_df_dict[bar_size] = pd.concat([self.__yesterday_top_gainier_minute_candle_df_dict[bar_size],
                                                                                      complete_df], axis=1)
        
        return self.__yesterday_top_gainier_minute_candle_df_dict[bar_size].loc[:, idx[contract_ticker_list, :]]
    
    def __get_daily_candle(self, contract_list: list, offset_day: int, outside_rth: bool = False, candle_retrieval_end_datetime: datetime.datetime = None) -> pd.DataFrame:
        candle_request_contract_list = []
        contract_ticker_list = [contract['symbol'] for contract in contract_list]
        daily_candle_df_ticker_list = list(self.__daily_canlde_df.columns.get_level_values(0).unique())

        for contract in contract_list:
            if contract['symbol'] not in daily_candle_df_ticker_list:
                candle_request_contract_list.append(contract)

        if candle_request_contract_list:    
            if outside_rth:
                outside_rth_str = 'true' 
            else:
                outside_rth_str = 'false'
            
            candle_df = self.__ib_connector.get_historical_candle_df(contract_list=candle_request_contract_list, 
                                                                     period=f'{offset_day}d', 
                                                                     bar_size=BarSize.ONE_DAY, 
                                                                     outside_rth=outside_rth_str, 
                                                                     candle_retrieval_end_datetime=candle_retrieval_end_datetime)
            if candle_df is not None and not candle_df.empty:
                complete_df = append_customised_indicator(candle_df)

                self.__daily_canlde_df = pd.concat([self.__daily_canlde_df,
                                                    complete_df], axis=1)

        result_df_ticker_list = self.__daily_canlde_df.columns.get_level_values(0).unique()
        select_contract_ticker_list = []
        
        for ticker in contract_ticker_list:
            if ticker in result_df_ticker_list:
                select_contract_ticker_list.append(ticker)
            else:
                logger.log_debug_msg(f'Exclude ticker {ticker} from daily_df, no historical data found')

        start_date_range = get_us_business_day(-offset_day, candle_retrieval_end_datetime).date() #should be -offset_day, -1 is just for test
        return self.__daily_canlde_df.loc[start_date_range:, idx[select_contract_ticker_list, :]]

    def __get_previous_day_top_gainers_contracts(self, offset: int = None, retrieval_end_datetime: datetime = None):
        retrieval_start_datetime = get_us_business_day(offset)
        
        if not retrieval_end_datetime:
            retrieval_end_datetime = retrieval_start_datetime
        
        previous_day_top_gainer_list = get_previous_day_top_gainer_list(self.__sqllite_connector, 
                                                                        PREVIOUS_DAY_TOP_GAINER_MIN_PCT_CHANGE, 
                                                                        retrieval_start_datetime, 
                                                                        retrieval_end_datetime)
        
        ticker_list = list(set([top_gainer[0] for top_gainer in previous_day_top_gainer_list]))

        previous_day_top_gainer_contract_list = self.__ib_connector.get_security_by_tickers(ticker_list)
        self.__ib_connector.update_contract_info(previous_day_top_gainer_contract_list)
        ticker_to_contract_dict = self.__ib_connector.get_ticker_to_contract_dict()
        
        contract_dict_list = []
        
        for ticker, contract in ticker_to_contract_dict.items():
            contract_dict_list.append({
                'symbol': ticker,
                'con_id': contract.con_id
            })
        
        return contract_dict_list
    