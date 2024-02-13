import math
import datetime
import pandas as pd 

from module.discord_chatbot_client import DiscordChatBotClient

from datasource.ib_connector import IBConnector
from datasource.finviz_connector import FinvizConnector
from sql.sqlite_connector import SqliteConnector

from pattern.initial_pop import InitialPop
from pattern.initial_dip import InitialDip

from model.discord.discord_message import DiscordMessage

from utils.filter_util import get_ib_scanner_filter, get_finviz_scanner_filter
from utils.nasdaq_data_util import get_all_ticker_in_the_market
from utils.datetime_util import PRE_MARKET_START_DATETIME, convert_us_to_hk_datetime, get_current_us_datetime, get_us_business_day
from utils.dataframe_util import append_customised_indicator
from utils.logger import Logger

from constant.scanner.scanner_target import ScannerTarget
from constant.candle.bar_size import BarSize
from constant.discord.discord_channel import DiscordChannel

idx = pd.IndexSlice
logger = Logger()

MAX_MARKET_CAP = 1e6 * 500
MIN_PRICE = 0.3
MAX_NO_OF_SCANNER_RESULT = 15
IB_TOP_GAINER_FILTER = get_ib_scanner_filter(ScannerTarget.TOP_GAINER,
                                       min_price = MIN_PRICE, 
                                       percent_change_param = 10, 
                                       min_usd_volume = 20000, 
                                       max_market_cap = MAX_MARKET_CAP, 
                                       additional_filter_list = [])
IB_TOP_LOSER_FILTER = get_ib_scanner_filter(ScannerTarget.TOP_LOSER,
                                      min_price = MIN_PRICE, 
                                      percent_change_param = -10, 
                                      min_usd_volume = 20000, 
                                      max_market_cap = MAX_MARKET_CAP, 
                                      additional_filter_list = [])
#IB_CLOSEST_TO_HALT_FILTER = get_ib_scanner_filter(ScanCode)

class Scanner:
    def __init__(self, discord_client: DiscordChatBotClient, ib_connector: IBConnector, finviz_connector: FinvizConnector, sqlite_connector: SqliteConnector) -> None:
        self.__discord_client = discord_client
        self.__ib_connector = ib_connector
        self.__finviz_connector = finviz_connector
        self.__sqllite_connector = sqlite_connector
        
        self.__all_ticker_list = get_all_ticker_in_the_market()
        self.__daily_canlde_df = pd.DataFrame()
        
        self.__yesterday_top_gainer_contract_list = self.__get_filtered_yesterday_top_gainer()
        self.__yesterday_top_gainier_minute_candle_df_dict = {
            BarSize.ONE_MINUTE: pd.DataFrame()
        }
        
    def scan_previous_days_gainer(self):
        pass
    
    def scan_yesterday_top_gainer(self):
        intra_day_one_minute_candle_df = self.__retrieve_intra_day_minute_candle(self.__yesterday_top_gainer_contract_list, BarSize.ONE_MINUTE)
        yesterday_one_minute_candle_df = self.__retrieve_yesterday_minute_candle(self.__yesterday_top_gainer_contract_list, BarSize.ONE_MINUTE)
        two_day_one_minute_candle_df = pd.concat([yesterday_one_minute_candle_df,
                                                  intra_day_one_minute_candle_df], axis=0)
        
        logger.log_debug_msg('scan yesterday top gainer df:', with_log_file=True, with_std_out=False)
        with pd.option_context('display.max_rows', None,
                   'display.max_columns', None,
                   'display.precision', 3,
                   ):
            logger.log_debug_msg(f'yesterday_one_minute_candle_df: {yesterday_one_minute_candle_df}', with_log_file=True, with_std_out=False)
            logger.log_debug_msg(f'two_day_one_minute_candle_df: {two_day_one_minute_candle_df}', with_log_file=True, with_std_out=False)
        
    def scan_top_gainer(self) -> None:
        contract_list = self.__ib_connector.get_screener_results(MAX_NO_OF_SCANNER_RESULT, IB_TOP_GAINER_FILTER)
        self.__ib_connector.update_contract_info(contract_list)
        one_minute_candle_df = self.__retrieve_intra_day_minute_candle(contract_list, BarSize.ONE_MINUTE)
        daily_df = self.__get_daily_candle(contract_list, 5, True)

        logger.log_debug_msg(f'top gainer scanner result: {[contract["symbol"] for contract in contract_list]}')
        self.__discord_client.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_GAINER_SCANNER_LIST)
        
        initial_pop_analyser = InitialPop(BarSize.ONE_MINUTE,
                                          one_minute_candle_df, 
                                          daily_df, 
                                          self.__ib_connector.get_ticker_to_contract_dict(), 
                                          self.__discord_client,
                                          self.__sqllite_connector)
        initial_pop_analyser.analyse()
            
    def scan_top_loser(self) -> None:
        contract_list = self.__ib_connector.get_screener_results(MAX_NO_OF_SCANNER_RESULT, IB_TOP_LOSER_FILTER)
        self.__ib_connector.update_contract_info(contract_list)
        one_minute_candle_df = self.__retrieve_intra_day_minute_candle(contract_list, BarSize.ONE_MINUTE)
        daily_df = self.__get_daily_candle(contract_list, 5, False)
        
        logger.log_debug_msg(f'top loser scanner result: {[contract["symbol"] for contract in contract_list]}')
        self.__discord_client.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_LOSER_SCANNER_LIST)
        
        initial_dip_analyser = InitialDip(BarSize.ONE_MINUTE,
                                          one_minute_candle_df, 
                                          daily_df, 
                                          self.__ib_connector.get_ticker_to_contract_dict(), 
                                          self.__discord_client,
                                          self.__sqllite_connector)
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
            candle_df = self.__ib_connector.get_historical_candle_df(contract_list, f'{math.floor(historical_data_interval_in_minute)}min', bar_size, 'true')
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
            candle_retrieval_start_dt = get_us_business_day(offset_day=-1)
            
            if outside_rth:
                outside_rth_str = 'true'
                candle_retrieval_start_dt = candle_retrieval_start_dt.replace(hour=20, minute=0, second=0, microsecond=0)
            else:
                outside_rth_str = 'false'
                candle_retrieval_start_dt = candle_retrieval_start_dt.replace(hour=16, minute=0, second=0, microsecond=0)

            candle_df = self.__ib_connector.get_historical_candle_df(candle_request_contract_list, '960min' , bar_size, outside_rth_str, candle_retrieval_start_dt)
            complete_df = append_customised_indicator(candle_df)
            
            self.__yesterday_top_gainier_minute_candle_df_dict[bar_size] = pd.concat([self.__yesterday_top_gainier_minute_candle_df_dict[bar_size],
                                                                                      complete_df], axis=1)
        
        return self.__yesterday_top_gainier_minute_candle_df_dict[bar_size].loc[:, idx[contract_ticker_list, :]]
    
    def __get_daily_candle(self, contract_list: list, offset_day: int, outside_rth: bool=False, candle_start_dt: datetime.datetime=None) -> pd.DataFrame:
        candle_request_contract_list = []
        contract_ticker_list = [contract['symbol'] for contract in contract_list]
        daily_candle_df_ticker_list = list(self.__daily_canlde_df.columns.get_level_values(0).unique())

        for contract in contract_list:
            if contract['symbol'] not in daily_candle_df_ticker_list:
                candle_request_contract_list.append(contract)

        if candle_start_dt:
            candle_retrieval_start_dt = candle_start_dt
        else:
            candle_retrieval_start_dt = get_current_us_datetime()

        if candle_request_contract_list:    
            if outside_rth:
                outside_rth_str = 'true'
                candle_retrieval_start_dt = candle_retrieval_start_dt.replace(hour=20, minute=0, second=0, microsecond=0)
            else:
                outside_rth_str = 'false'
                candle_retrieval_start_dt = candle_retrieval_start_dt.replace(hour=16, minute=0, second=0, microsecond=0)

            candle_df = self.__ib_connector.get_historical_candle_df(candle_request_contract_list, f'{offset_day + 1}d' , BarSize.ONE_DAY, outside_rth_str, candle_retrieval_start_dt)
            complete_df = append_customised_indicator(candle_df)
           
            self.__daily_canlde_df = pd.concat([self.__daily_canlde_df,
                                                complete_df], axis=1)
       
        start_date_range = get_us_business_day(-offset_day, candle_retrieval_start_dt).date()
        return self.__daily_canlde_df.loc[start_date_range:, idx[contract_ticker_list, :]]

    def __get_filtered_yesterday_top_gainer(self):
        YESTERDAY_TOP_GAINER_MIN_PCT_CHANGE = 30
        
        filtered_yesterday_top_gainer_ticker_list = []
        yesterday_top_gainer_snapshot_list = self.__finviz_connector.get_yesterday_top_gainer()
        
        for top_gainer in yesterday_top_gainer_snapshot_list:
            if top_gainer.change_pct >= YESTERDAY_TOP_GAINER_MIN_PCT_CHANGE:
                filtered_yesterday_top_gainer_ticker_list.append(top_gainer.ticker)
        
        yesterday_top_gainer_contract_list = self.__ib_connector.get_security_by_tickers(filtered_yesterday_top_gainer_ticker_list)
        self.__ib_connector.update_contract_info(yesterday_top_gainer_contract_list)
        ticker_to_contract_dict = self.__ib_connector.get_ticker_to_contract_dict()
        
        contract_dict_list = []
        
        for ticker, contract in ticker_to_contract_dict.items():
            contract_dict_list.append({
                'symbol': ticker,
                'con_id': contract.con_id
            })
        
        return contract_dict_list
    
    
    
    
    
    # def __retrieve_yesterday_one_minute_candle(self):
    #     pass
        
    # def __get_previous_days_gainer_contract_list(self):
    
    # PREVIOUS_GAINER_OBSERVE_DAY = 6
    # PREVIOUS_DAYS_TOP_GAINER_MIN_GAP_UP_PCT = 5
    # PREVIOUS_DAYS_TOP_GAINER_MIN_PCT_CHANGE = 20
    
    #     logger.log_debug_msg(f'No. of total tickers: {len(self.__all_ticker_list)}')
    #     security_contract_list = self.__ib_connector.get_security_by_tickers(self.__all_ticker_list)
    #     self.__ib_connector.update_contract_info(security_contract_list)
    #     ticker_to_contract_dict = self.__ib_connector.get_ticker_to_contract_dict()
    #     con_id_list = []
    #     for ticker, contract in ticker_to_contract_dict.items():
    #         logger.log_debug_msg(f'Ticker: {ticker}, Contract: {contract}')
    #         below_max_market_cap = False
    #         above_min_price = False
            
    #         if contract.numeric_market_cap:
    #             if contract.numeric_market_cap <= MAX_MARKET_CAP:
    #                 below_max_market_cap = True
                    
    #         if contract.snapshot.last >= MIN_PRICE or contract.snapshot.previous_close >= MIN_PRICE:
    #             above_min_price = True
            
    #         if below_max_market_cap and above_min_price:      
    #             con_id_list.append(contract.con_id) 
    #     logger.log_debug_msg(f'No. of filtered contracts: {len(con_id_list)}')           
    #     # save to db in case of ...
        
    #     #start_dt = datetime.datetime.now().astimezone(US_EASTERN_TIMEZONE).replace(day=5, hour=20, minute=0, second=0, microsecond=0)
    #     #start_dt = convert_us_to_hk_datetime(datetime.datetime.now())
    #     start_dt = get_current_us_datetime()
    #     candle_df = self.__ib_connector.get_historical_candle_df(con_id_list, f'{PREVIOUS_GAINER_OBSERVE_DAY}d', BarSize.ONE_DAY, 'true', start_dt) 
    #     completed_df = append_customised_indicator(candle_df)
    #     gap_pct_df = completed_df.loc[:, idx[:, CustomisedIndicator.GAP_PCT_CHANGE.value]].rename(columns={CustomisedIndicator.GAP_PCT_CHANGE.value: RuntimeIndicator.COMPARE.value})
    #     close_change_df = completed_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]].rename(columns={CustomisedIndicator.CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
    #     low_df = completed_df.loc[:, idx[:, Indicator.LOW.value]].rename(columns={Indicator.LOW.value: RuntimeIndicator.COMPARE.value})
    #     volume_df = completed_df.loc[:, idx[:, Indicator.VOLUME.value]]
    #     estimated_min_usd_volume_df = low_df.mul(volume_df.values)
    #     result_boolean_df = (gap_pct_df >= TOP_GAINER_MIN_GAP_UP_PCT) & (close_change_df >= TOP_GAINER_MIN_PCT_CHANGE)
    #     previous_gap_up_gainer_result_series = result_boolean_df.any()
    #     previous_gap_up_gainer_ticker_list = previous_gap_up_gainer_result_series.index[previous_gap_up_gainer_result_series].get_level_values(0).tolist()
        
    #     for ticker in previous_gap_up_gainer_ticker_list:
    #         symbol = ticker_to_contract_dict[ticker].symbol
    #         con_id = ticker_to_contract_dict[ticker].con_id
            
    #         self.__swing_contract_watchlist.append({
    #             'symbol': symbol,
    #             'con_id': con_id
    #         })
            