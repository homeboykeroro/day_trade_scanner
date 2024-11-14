from asyncio import AbstractEventLoop
import math
import re
import threading
import time
from datetime import time as dt_time, datetime, timedelta
import html
import oracledb
import pytz
from aiohttp import ClientError
from requests import HTTPError, RequestException
import requests
import numpy as np
import pandas as pd
from exception.ib_data_retrieval_timeout_error import IBDataRetrievalTimeoutError
import urllib3

from model.ib.contract_info import ContractInfo
from model.ib.snapshot import Snapshot

from utils.common.config_util import get_config
from utils.common.http_util import send_async_request
from utils.common.collection_util import get_chunk_list
from utils.sql.api_endpoint_lock_record_util import check_api_endpoint_locked, update_api_endpoint_lock
from utils.common.dataframe_util import append_customised_indicator
from utils.common.datetime_util import  PRE_MARKET_START_DATETIME, US_BUSINESS_DAY, get_us_business_day, get_current_us_datetime
from utils.logger import Logger

from constant.endpoint.ib.client_portal_api_endpoint import ClientPortalApiEndpoint
from constant.candle.bar_size import BarSize
from constant.indicator.indicator import Indicator

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = Logger()
session = requests.Session()
idx = pd.IndexSlice

# Field     Return Type   Description
# 55	    string	      Symbol
# 7221	    string	      Listing Exchange
# 7051	    string	      Company name
# 7289	    string	      Market Cap
# 7636	    number	      Shortable inventory
# 7637	    string	      Fee rebate rate
# 7644	    string	      Shortable - Describes the level of difficulty with which the security can be sold short.
# 79	    string	      Realized PnL
# 80	    string	      Unrealized PnL %
# 83	    string	      Change %
# 7674	    string	      EMA(200)
# 7675	    string	      EMA(100) - Exponential moving average (N=100).
# 7676	    string	      EMA(50) - Exponential moving average (N=50).
# 7677	    string	      EMA(20) - Exponential moving average (N=20).
# 7678	    string	      Price/EMA(200) - Price to Exponential moving average (N=200) ratio -1, displayed in percents.
# 7679	    string	      Price/EMA(100) - Price to Exponential moving average (N=100) ratio -1, displayed in percents.
# 7680	    string	      Price/EMA(50) - Price to Exponential moving average (N=50) ratio -1, displayed in percents.
# 7681	    string	      Price/EMA(20) - Price to Exponential moving average (N=20) ratio -1, displayed in percents.
# 6509      string        A multi-character value representing the Market Data Availability.
# 31        string        Last Price
# 70	    string	      High - Current day high price
# 71	    string	      Low - Current day low price
# 7295	    string	      Open - Today’s opening price.
# 7296	    string	      Close - Today’s closing price.
# 7741	    string	      Prior Close - Yesterday’s closing price
# 83	    string	      Change % - The difference between the last price and the close on the previous trading day in percentage.
# 7284	    string	      Historic Volume (30d)
# 7672	    string	      Dividends TTM	- This value is the total of the expected dividend payments over the last twelve months per share.
SNAPSHOT_FIELD_LIST_STR = '55,7221,7051,7289,7644,7636,7637,6509,31,7741'

# Snapshot API Endpoint Limit
SNAPSHOT_RETRIEVAL_TIMEOUT_PERIOD = get_config('SYS_PARAM', 'SNAPSHOT_RETRIEVAL_TIMEOUT_PERIOD')
MAX_SNAPSHOT_REQUEST_PER_SECOND = get_config('SYS_PARAM', 'MAX_SNAPSHOT_REQUEST_PER_SECOND')
MAX_CONCURRENT_SNAPSHOT_REQUEST = get_config('SYS_PARAM', 'MAX_CONCURRENT_SNAPSHOT_REQUEST')

# Default API Endpoint Limit
DEFAULT_MAX_REQUEST_PER_SECOND = get_config('SYS_PARAM', 'DEFAULT_MAX_REQUEST_PER_SECOND')
DEFAULT_MAX_CONCURRENT_REQUEST = get_config('SYS_PARAM', 'DEFAULT_MAX_CONCURRENT_REQUEST')
DEFAULT_API_ENDPOINT_LOCK_CHECK_INTERVAL = get_config('SYS_PARAM', 'DEFAULT_API_ENDPOINT_LOCK_CHECK_INTERVAL')

CON_ID_CONCAT_CHUNK_SIZE = get_config('SYS_PARAM', 'CON_ID_CONCAT_CHUNK_SIZE')
class IBConnector:
    def __init__(self, loop: AbstractEventLoop = None) -> None:
        self.__ticker_to_contract_info_dict = {}
        self.__loop = loop
        
        self.__daily_canlde_df = pd.DataFrame()
    
    def fetch_contract_by_ticker_list(self, ticker_list: list, security_api_endpoint_lock_check_interval: int):
        self.acquire_api_endpoint_lock(ClientPortalApiEndpoint.SECURITY_STOCKS_BY_SYMBOL, security_api_endpoint_lock_check_interval)
        logger.log_debug_msg(f'Fetch secuity for {ticker_list}')
        contract_list = self.get_security_by_tickers(ticker_list)
        self.release_api_endpoint_lock(ClientPortalApiEndpoint.SECURITY_STOCKS_BY_SYMBOL)

        return contract_list
    
    def fetch_screener_result(self, screener_filter: dict, max_no_of_scanner_result: int, scanner_api_endpoint_lock_check_interval: int) -> list:
        # Get contract list from IB screener
        self.acquire_api_endpoint_lock(ClientPortalApiEndpoint.RUN_SCANNER, scanner_api_endpoint_lock_check_interval)
        logger.log_debug_msg(f'Fetch {screener_filter.get("type")} screener result', with_std_out=True)
        contract_list = self.get_screener_results(screener_filter, max_no_of_scanner_result)
        self.release_api_endpoint_lock(ClientPortalApiEndpoint.RUN_SCANNER)
        
        return contract_list
    
    def fetch_snapshot(self, contract_list: list, snapshot_api_endpoint_lock_check_interval: int) -> dict:
        if (self.check_if_contract_update_required(contract_list)):
            self.acquire_api_endpoint_lock(ClientPortalApiEndpoint.SNAPSHOT, snapshot_api_endpoint_lock_check_interval)
            logger.log_debug_msg(f'Fetch snapshot for {[contract.get("symbol") for contract in contract_list]}', with_std_out=True)
            self.update_contract_info(contract_list)
            self.release_api_endpoint_lock(ClientPortalApiEndpoint.SNAPSHOT)
        
        ticker_to_contract_dict = self.get_ticker_to_contract_dict()
        return ticker_to_contract_dict
    
    def fetch_daily_candle(self, contract_list: list, offset_day: int, market_data_api_endpoint_lock_check_inverval: int) -> pd.DataFrame:
        self.acquire_api_endpoint_lock(ClientPortalApiEndpoint.MARKET_DATA_HISTORY, market_data_api_endpoint_lock_check_inverval)
        daily_df = self.get_daily_candle(contract_list=contract_list, 
                                         offset_day=offset_day, 
                                         outside_rth=False)
        self.release_api_endpoint_lock(ClientPortalApiEndpoint.MARKET_DATA_HISTORY)
        
        return daily_df

    def fetch_intra_day_minute_candle(self, contract_list: list, market_data_api_endpoint_lock_check_interval: int) -> pd.DataFrame:
        self.acquire_api_endpoint_lock(ClientPortalApiEndpoint.MARKET_DATA_HISTORY, market_data_api_endpoint_lock_check_interval)
        one_minute_candle_df = self.retrieve_intra_day_minute_candle(contract_list=contract_list, 
                                                                     bar_size=BarSize.ONE_MINUTE)
        self.release_api_endpoint_lock(ClientPortalApiEndpoint.MARKET_DATA_HISTORY)
        
        return one_minute_candle_df
    
    def get_daily_candle(self, contract_list: list, 
                               offset_day: int, 
                               outside_rth: bool = False, 
                               candle_retrieval_end_datetime: datetime = None) -> pd.DataFrame:
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
            
            candle_df = self.get_historical_candle_df(contract_list=candle_request_contract_list, 
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
                logger.log_debug_msg(f'Exclude ticker {ticker} from daily_df, no historical data found', with_std_out=True)

        start_date_range = get_us_business_day(-offset_day, candle_retrieval_end_datetime).date()
        return self.__daily_canlde_df.loc[start_date_range:, idx[select_contract_ticker_list, :]]
        
    def retrieve_intra_day_minute_candle(self, contract_list: list, bar_size: BarSize) -> pd.DataFrame:
        us_current_datetime = get_current_us_datetime().replace(microsecond=0, second=0)
        historical_data_interval_in_minute = (us_current_datetime - PRE_MARKET_START_DATETIME).total_seconds() / 60
        logger.log_debug_msg(f'Historical candle data retrieval period: {historical_data_interval_in_minute} minutes')

        if historical_data_interval_in_minute < 1:
            logger.log_debug_msg('Historical candle data retrieval retrieval period is less than 1 minute', with_std_out=True)
            return None
        else:
            candle_df = self.get_historical_candle_df(contract_list=contract_list, 
                                                      period=f'{math.floor(historical_data_interval_in_minute)}min', 
                                                      bar_size=bar_size, 
                                                      outside_rth='true')
            
            if candle_df is not None and not candle_df.empty:
                return append_customised_indicator(candle_df)
            else:
                return pd.DataFrame()
    
    def acquire_api_endpoint_lock(self, endpoint: ClientPortalApiEndpoint, check_interval: int = DEFAULT_API_ENDPOINT_LOCK_CHECK_INTERVAL):
        logger.log_debug_msg(f'Acquiring lock for {endpoint}', with_std_out=True)
        
        try:
            while check_api_endpoint_locked(endpoint):
                time.sleep(check_interval)
                logger.log_debug_msg(f'{endpoint.value} is locking', with_std_out=True)
                continue
            
            self.set_api_endpoint_lock(endpoint, True)
        except Exception as e:
            logger.log_error_msg(f'Failed to acquire lock for {endpoint}, {e}', with_std_out=True)
            raise oracledb.Error(f'Failed to acquire lock for {endpoint}, {e}')
    
    def release_api_endpoint_lock(self, endpoint: ClientPortalApiEndpoint):
        try:
            self.set_api_endpoint_lock(endpoint, False)
            logger.log_debug_msg(f'{endpoint.value} lock released', with_std_out=True)
        except Exception as e:
            logger.log_error_msg(f'Failed to release lock for {endpoint}, {e}', with_std_out=True)
            raise oracledb.Error(f'Failed to release lock for {endpoint}, {e}')
    
    def set_api_endpoint_lock(self, endpoint: ClientPortalApiEndpoint, lock: bool):
        try:
            update_lock_start_time = time.time()

            locked_by = None
            lock_datetime = None
            is_locked = 'N'
            
            if lock:
                locked_by = threading.current_thread().name
                lock_datetime = get_current_us_datetime()
                is_locked = 'Y'
                
            update_api_endpoint_lock([dict(is_locked=is_locked, 
                                           locked_by=locked_by, 
                                           lock_datetime=lock_datetime, 
                                           endpoint=endpoint.value)])
            
            logger.log_debug_msg(f'Update lock time: {time.time() - update_lock_start_time} seconds')
        except Exception as e:
            logger.log_error_msg(f'Update {endpoint} lock error, {e}', with_std_out=True)
            raise oracledb.Error(f'Update {endpoint} lock error, {e}')
    
    def receive_brokerage_account(self):
        try:
            receive_brokerage_account_time = time.time()
            brokerage_account_response = session.get(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.ACCOUNT}', verify=False)
            logger.log_debug_msg(f'Receive brokerage account response time: {time.time() - receive_brokerage_account_time} seconds')
            brokerage_account_response.raise_for_status()
        except requests.exceptions.HTTPError as brokerage_account_request_exception:
            raise brokerage_account_request_exception
        else:
            brokerage_account = brokerage_account_response.json()
            
            if brokerage_account.get('accounts'):
                logger.log_debug_msg('Brokerage account retrieval success', with_std_out = True)
            else:
                raise requests.RequestException('No brokerage account information is found')
    
    def reauthenticate(self):
        reauthenticate_time = time.time()
        
        try:
            sso_validate_response = session.get(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SSO_VALIDATE}', verify=False)
            reauthenticate_response = session.post(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.REAUTHENTICATE}', verify=False)
            logger.log_debug_msg(f'Session re-authentication response time: {time.time() - reauthenticate_time} seconds')

            sso_validate_response.raise_for_status()
            reauthenticate_response.raise_for_status()
            logger.log_debug_msg(f'SSO validation result: {sso_validate_response.json()}', with_std_out=True)
                
            reauthenticate_result = reauthenticate_response.json()
            reauthenticate_message = reauthenticate_result.get('message')
            
            is_sso_validation_successful = sso_validate_response.json().get('RESULT')
            
            if not is_sso_validation_successful:
                raise RequestException('SSO validation failed')
            
            if not reauthenticate_message:
                raise RequestException('Failed to reauthenticate session')
        except (RequestException, ClientError, HTTPError)  as reauthenticate_exception:
            logger.log_error_msg(f'Reauthentication falied, {reauthenticate_exception}')
            raise reauthenticate_exception
        except Exception as exception:
            logger.log_error_msg(f'Reauthentication and SSO validation fatal error, {exception}')
            raise Exception(f'Reauthentication and SSO validation fatal error, {exception}')
    
    def check_auth_status(self):
        is_connection_success = True

        try:
            check_status_start_time = time.time()
            status_response = session.post(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.AUTH_STATUS}', verify=False)
            logger.log_debug_msg(f'Check authentication status response time: {time.time() - check_status_start_time} seconds')
            status_response.raise_for_status()
        except requests.exceptions.HTTPError as check_status_request_exception:
            raise check_status_request_exception
        else:
            status_result = status_response.json()

            authenticated = status_result['authenticated']
            competing = status_result['competing']
            connected = status_result['connected']

            if 'message' in status_result:
                message = status_result['message']
            else:
                message = None

            if not authenticated:
                is_connection_success = False
                logger.log_error_msg('Session is not authenticated')

            if not connected:
                is_connection_success = False
                logger.log_error_msg('Session is not connected')

            if competing:
                is_connection_success = False
                logger.log_error_msg('Session is occupied')

            if is_connection_success:
                logger.log_debug_msg('Connected and authenticated', with_std_out = True)
            else:
                error_msg = 'Client portal API connection failed'
                if message:
                    error_msg += f', {message}'

                raise requests.RequestException(error_msg)
    
    def get_ticker_to_contract_dict(self):
        return self.__ticker_to_contract_info_dict
    
    def get_screener_results(self, scanner_filter: dict, max_no_of_scanner_result: int) -> list:
        try:
            scanner_type = scanner_filter.get("type")
            scanner_request_start_time = time.time()
            
            scanner_response = session.post(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.RUN_SCANNER}', json=scanner_filter, verify=False)
            logger.log_debug_msg(f'{scanner_type} scanner result response time: {time.time() - scanner_request_start_time} seconds')
            scanner_response.raise_for_status()
        except requests.exceptions.HTTPError as scanner_request_exception:
            raise requests.exceptions.HTTPError(f'Error occurred while requesting {scanner_type} scanner result')
        else:
            logger.log_debug_msg(f'{scanner_type} scanner full result json: {[contract.get("symbol") for contract in scanner_response.json().get("contracts")]}')
            logger.log_debug_msg(f'{scanner_type} scanner full result size: {len(scanner_response.json().get("contracts"))}') #bug fix Add 
            logger.log_debug_msg(f'Maximum {scanner_type} scanner result size: {max_no_of_scanner_result}')
            scanner_result = scanner_response.json()['contracts']
            scanner_result_without_otc_stock = []
        
            for result in scanner_result:
                if 'symbol' in result:
                    symbol = result['symbol']

                    if re.match('^[A-Z]{1,4}$', symbol): 
                        scanner_result_without_otc_stock.append(result)
                    else:
                        logger.log_debug_msg(f'Exclude {symbol} from scanner result')
                else:
                    logger.log_debug_msg(f'Exclude unknown contract from scanner result, {result}', with_std_out=True)
            
            return scanner_result_without_otc_stock[:max_no_of_scanner_result]
    
    def get_security_by_tickers(self, ticker_list: list) -> list:
        result_list = []
        get_security_payload_list = []
        temp_list = []
        ticker_chunk_list = get_chunk_list(ticker_list, CON_ID_CONCAT_CHUNK_SIZE)

        for ticker_chunk in ticker_chunk_list:
            symbol_list = []
            symbol_list.extend(ticker_chunk)
            
            get_security_payload = {
                'symbols': ','.join(symbol_list)
            }

            get_security_payload_list.append(get_security_payload)

        try:
            get_security_by_ticker_start_time = time.time()
            security_response = send_async_request(method='GET', 
                                                   endpoint=f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SECURITY_STOCKS_BY_SYMBOL}', 
                                                   payload_list=get_security_payload_list, 
                                                   chunk_size=DEFAULT_MAX_CONCURRENT_REQUEST,
                                                   no_of_request_per_sec=DEFAULT_MAX_REQUEST_PER_SECOND,
                                                   loop=self.__loop)
            logger.log_debug_msg(f'Get security by ticker response time: {time.time() - get_security_by_ticker_start_time}')
        except Exception as security_request_exception:
            logger.log_error_msg(f'Error occurred while requesting security by ticker, Cause: {security_request_exception}')
            raise security_request_exception
        else:
            for ticker_to_security_list_dict in security_response:
                for ticker, security_list in ticker_to_security_list_dict.items():
                    if ticker == 'error':
                        raise requests.RequestException('Error occurred while requesting security by ticker')
                    
                    if security_list:
                        contract_found_no = 0
                        for security in security_list:
                            contract_list = security['contracts']
                            contract_found = False

                            for contract in contract_list:
                                if contract.get('isUS'):
                                    result_list.append({
                                        'con_id': contract['conid'],
                                        'symbol': ticker
                                    })
                                    contract_found = True
                                    contract_found_no = 1
                                    #logger.log_debug_msg(f'Add contract {contract} found by ticker {ticker}')
                                    break
                            if contract_found:
                                break
                            
                        if contract_found_no == 0:
                            temp_list.append(ticker)
                            logger.log_debug_msg(f'Test Can\'t find security for ticker of "{ticker}"')
                    else:
                        logger.log_debug_msg(f'Can\'t find security for ticker of "{ticker}"')

            return result_list
    
    def check_if_contract_update_required(self, contract_list: list):
        need_update = False
        
        for contract in contract_list:   ########### need to extract this method
            ticker_symbol = contract['symbol']

            if ticker_symbol not in self.__ticker_to_contract_info_dict:
                need_update = True
                break
        
        return need_update
    
    def update_contract_info(self, contract_list: list) -> None:
        update_contract_info_start_time = time.time()
        snapshot_data_con_id_list = []
        
        for contract in contract_list:
            con_id = contract['con_id']
            ticker_symbol = contract['symbol']

            if ticker_symbol not in self.__ticker_to_contract_info_dict:
                snapshot_data_con_id_list.append(con_id)
        
        if snapshot_data_con_id_list:
            self.update_snapshot(snapshot_data_con_id_list)
            self.update_sec_def(snapshot_data_con_id_list)
        else:
            logger.log_debug_msg(f'No snapshot data is required to update for the contracts: {contract_list}')
        
        logger.log_debug_msg(f'update contract info completed time: {time.time() - update_contract_info_start_time} seconds')
    
    def update_snapshot(self, con_id_list: list) -> None:
        if not con_id_list:
            return
        
        logger.log_debug_msg(f'Getting market cap, is shortable, shortable shares, and rebate rate data, conId list: {con_id_list}')
        snapshot_payload_list = []

        chunk_list = get_chunk_list(con_id_list, CON_ID_CONCAT_CHUNK_SIZE)
        
        for chunk in chunk_list:
            get_snapshot_payload = {
                'conids': ','.join(str(con_id) for con_id in chunk),
                'fields' : SNAPSHOT_FIELD_LIST_STR
            }
            snapshot_payload_list.append(get_snapshot_payload)

        snapshot_retrieval_start_time = time.time()
        
        try:
            snapshot_retrieval_success = False
            
            while not snapshot_retrieval_success:
                incomplete_snapshot_response_list = []
                get_contract_snapshot_start_time = time.time()
                snapshot_response_list = send_async_request(method='GET', 
                                                            endpoint=f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SNAPSHOT}', 
                                                            payload_list=snapshot_payload_list, 
                                                            chunk_size=MAX_CONCURRENT_SNAPSHOT_REQUEST,
                                                            no_of_request_per_sec=MAX_SNAPSHOT_REQUEST_PER_SECOND,
                                                            loop=self.__loop)
                logger.log_debug_msg(f'Get market cap, is shortable, shortable shares, and rebate rate data response time: {time.time() - get_contract_snapshot_start_time}')
                
                for snapshot_list in snapshot_response_list:
                    for snapshot_data in snapshot_list:
                        if '_updated' not in snapshot_data:
                            logger.log_debug_msg(f'Incomplete snapshot response found: {snapshot_data}')
                            logger.log_debug_msg(f'Full snapshot response list: {snapshot_response_list}')
                            incomplete_snapshot_response_list.append(snapshot_data)
                
                retrieval_time_min = (time.time() - snapshot_retrieval_start_time) / 60
                
                if incomplete_snapshot_response_list:
                    if retrieval_time_min > SNAPSHOT_RETRIEVAL_TIMEOUT_PERIOD:
                        raise IBDataRetrievalTimeoutError(f'Snapshot retrieval timeout, incompleted snapshot responst list: {incomplete_snapshot_response_list}')
                    
                    logger.log_debug_msg(f'Incomplete data found, incompleted snapshot response list: {incomplete_snapshot_response_list}, full snapshot response list: {snapshot_response_list}')
                    logger.log_debug_msg('Re-fetch snapshot after 0.5 second', with_std_out=True)
                    time.sleep(0.5)
                else:
                    logger.log_debug_msg('No incompleted snapshot response found', with_std_out=True)
                    snapshot_retrieval_success = True
                          
        except Exception as snapshot_request_exception:
            logger.log_error_msg(f'Error occurred while requesting market cap, is shortable, shortable shares, and rebate rate data, Cause: {snapshot_request_exception}')
            raise snapshot_request_exception
        else:
            logger.log_debug_msg('Snapshot updated successfully', with_std_out=True)
            for snapshot_list in snapshot_response_list:
                for snapshot_data in snapshot_list:
                    con_id = snapshot_data['conid']

                    if '55' in snapshot_data:
                        symbol = snapshot_data['55']
                    else:
                        symbol = None
                        logger.log_debug_msg(f'No ticker symbol data is available for {snapshot_data}')

                    if '7051' in snapshot_data:
                        company_name = snapshot_data['7051']
                    else:
                        company_name = None
                        logger.log_debug_msg(f'No company name data is available for {snapshot_data}')

                    if '7289' in snapshot_data:
                        market_cap = snapshot_data['7289']
                    else:
                        market_cap = None
                        logger.log_debug_msg(f'No market cap data is available for {snapshot_data}')

                    if '7221' in snapshot_data:
                        exchange = snapshot_data['7221']
                    else:
                        exchange = None
                        logger.log_debug_msg(f'No exchange cap data is available for {snapshot_data}')

                    if '7644' in snapshot_data:
                        shortable = snapshot_data['7644']
                    else:
                        shortable = None
                        logger.log_debug_msg(f'No shortable cap data is available for {snapshot_data}')

                    if '7636' in snapshot_data:
                        shortable_shares = snapshot_data['7636']
                    else:
                        shortable_shares = None
                        logger.log_debug_msg(f'No shortable shares data is available for {snapshot_data}')

                    if '7637' in snapshot_data:
                        rebate_rate = snapshot_data['7637']
                    else:
                        rebate_rate = None
                        logger.log_debug_msg(f'No rebate rate data is available for {snapshot_data}')

                    if '6509' in snapshot_data:
                        market_data_availability = snapshot_data['6509']
                    else:
                        market_data_availability = None
                        logger.log_debug_msg(f'No market data availability data is available for {snapshot_data}')
                    
                    if '31' in snapshot_data:
                        last = snapshot_data['31']
                    else:
                        last = None
                        logger.log_debug_msg(f'No last price data is available for {snapshot_data}')
                    
                    if '31' in snapshot_data:
                        last = snapshot_data['31']
                    else:
                        last = None
                        logger.log_debug_msg(f'No last price data is available for {snapshot_data}')
                        
                    if '7741' in snapshot_data:
                        previous_close = snapshot_data['7741']
                    else:
                        previous_close = None
                        logger.log_debug_msg(f'No previous close data is available for {snapshot_data}')
                    
                    snapshot = Snapshot(market_data_availability, last, previous_close)

                    if symbol:
                        contract_info = ContractInfo(con_id, symbol, exchange, company_name, None, market_cap, shortable, shortable_shares, rebate_rate, snapshot)
                        self.__ticker_to_contract_info_dict[symbol] = contract_info

    def update_sec_def(self, con_id_list: list) -> None:
        if not con_id_list:
            return
        
        logger.log_debug_msg(f'Getting sector data, conId list: {con_id_list}')
        sec_def_payload_list = []
        
        chunk_list = get_chunk_list(con_id_list, CON_ID_CONCAT_CHUNK_SIZE)
        
        for chunk in chunk_list:
            get_sec_def_payload = {
                'conids': ','.join(str(con_id) for con_id in chunk)
            }
            sec_def_payload_list.append(get_sec_def_payload)

        try:
            get_security_definitions_start_time = time.time()
            sec_def_response_list = send_async_request(method='GET', 
                                                       endpoint=f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SECURITY_DEFINITIONS}', 
                                                       payload_list=sec_def_payload_list, 
                                                       chunk_size=100,
                                                       loop=self.__loop)
            logger.log_debug_msg(f'Get sector data response time: {time.time() - get_security_definitions_start_time}')
        except Exception as snapshot_request_exception:
            logger.log_error_msg(f'Error occurred while requesting sector data, Cause: {snapshot_request_exception}')
            raise snapshot_request_exception
        else:
            for sec_def_response in sec_def_response_list:
                sef_def_list = sec_def_response['secdef']

                for sec_df in sef_def_list:
                    if not sec_df:
                        continue
                    
                    ticker = sec_df['ticker']

                    if 'group' in sec_df:
                        group = html.unescape(sec_df['group']) if sec_df['group'] else None
                    else:
                        group = None
                        logger.log_debug_msg(f'{ticker} has no group')

                    if 'sectorGroup' in sec_df:
                        sector_group = html.unescape(sec_df['sectorGroup']) if sec_df['sectorGroup'] else None
                    else:
                        sector_group = None
                        logger.log_debug_msg(f'{ticker} has no sector group')

                    if ticker in self.__ticker_to_contract_info_dict:
                        self.__ticker_to_contract_info_dict[ticker].sector = f'{sector_group}, {group}'
                    else:
                        logger.log_debug_msg(f'{ticker} does not exist in ticker to contract dict')

    def get_historical_candle_df(self, contract_list: list, 
                                       period: str, 
                                       bar_size: BarSize, 
                                       outside_rth: str = 'true', 
                                       candle_retrieval_end_datetime: datetime = None) -> pd.DataFrame:
        con_id_list = [contract['con_id'] for contract in contract_list]
        ticker_list = [contract['symbol'] for contract in contract_list]
        
        candle_payload_list = []
        request_start_time = None
        
        if bar_size.value.endswith('d'):  
            subtract_day = int(period[:-1])
            
            if not candle_retrieval_end_datetime:
                candle_retrieval_end_datetime = get_us_business_day(0) if get_us_business_day(0).time() > dt_time(16, 0, 0) else get_us_business_day(-1)
                
            if outside_rth == 'true':
                candle_retrieval_end_datetime = candle_retrieval_end_datetime.replace(hour=20, minute=0, second=0, microsecond=0)
            else:  
                candle_retrieval_end_datetime = candle_retrieval_end_datetime.replace(hour=16, minute=0, second=0, microsecond=0)

            datetime_idx_range_end_datetime = candle_retrieval_end_datetime.date()
            datetime_idx_range_start_datetime = get_us_business_day(offset_day=-subtract_day, us_date=candle_retrieval_end_datetime).date()
            request_start_time = get_us_business_day(1, candle_retrieval_end_datetime)
            period = f'{subtract_day + 1}d'
            interval = 'D'
        else:
            if not candle_retrieval_end_datetime:
                datetime_idx_range_end_datetime = get_current_us_datetime().replace(second=0, microsecond=0, tzinfo=None)
            else:
                datetime_idx_range_end_datetime = candle_retrieval_end_datetime.replace(second=0, microsecond=0, tzinfo=None)
                request_start_time = datetime_idx_range_end_datetime
            
            if period.endswith('d'):
                subtract_day = int(period[:-1])
                datetime_idx_range_start_datetime = datetime_idx_range_end_datetime - timedelta(days=(subtract_day - 1))
            elif period.endswith('min'):
                subtract_minute = int(period[:-3])
                datetime_idx_range_start_datetime = datetime_idx_range_end_datetime - timedelta(minutes=(subtract_minute))
            
            interval = bar_size.value
        
        for con_id in con_id_list:
            candle_payload = {
                'conid': str(con_id),
                'period': period,
                'bar': bar_size.value,
                'outsideRth': outside_rth,
            }

            if request_start_time:
                if not request_start_time.tzinfo:
                    us_timezone = pytz.timezone('US/Eastern')
                    request_start_time = us_timezone.localize(request_start_time)
                    
                candle_payload['startTime'] = request_start_time.astimezone(pytz.utc).strftime('%Y%m%d-%H:%M:%S')
            
            candle_payload_list.append(candle_payload)

        try:
            logger.log_debug_msg(f'Getting {bar_size.value} historical candle data, paylaod list: {candle_payload_list}')
            get_one_minute_candle_start_time = time.time()
            candle_response_list = send_async_request(method='GET', 
                                                      endpoint=f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.MARKET_DATA_HISTORY}', 
                                                      payload_list=candle_payload_list, 
                                                      chunk_size=5,
                                                      loop=self.__loop)
            logger.log_debug_msg(f'Get {bar_size.value} historical candle data time: {time.time() - get_one_minute_candle_start_time}')
        except Exception as historical_data_request_exception:
            logger.log_error_msg(f'An error occurred while requesting {bar_size.value} historical data, Cause: {historical_data_request_exception}')
            raise historical_data_request_exception
        else:
            construct_dataframe_start_time = time.time()

            logger.log_debug_msg(f'Create datetime range index, start datetime: {datetime_idx_range_start_datetime}, end datetime: {datetime_idx_range_end_datetime}')
            datetime_range_index = pd.date_range(start=datetime_idx_range_start_datetime, end=datetime_idx_range_end_datetime, freq=interval)

            ticker_candle_df_list = []

            for historical_data in candle_response_list:
                if 'error' in historical_data:
                    continue
                
                if 'symbol' not in historical_data:
                    continue
                
                ticker = historical_data['symbol']
                historical_ohlcv_list = historical_data['data']

                ohlcv_list = []
                datetime_idx_list = []

                for historical_ohlcv in historical_ohlcv_list:
                    open = historical_ohlcv['o']
                    high = historical_ohlcv['h']
                    low = historical_ohlcv['l']
                    close = historical_ohlcv['c']
                    volume = int(historical_ohlcv['v'] * 100)
                    dt = historical_ohlcv['t']
                    ohlcv_list.append([open, high, low, close, volume])
                    datetime_idx_list.append(dt)

                if bar_size.value.endswith('d'):
                    datetime_index = (pd.DatetimeIndex(pd.to_datetime(datetime_idx_list, unit='ms', utc=False)
                                                         .tz_localize('UTC')
                                                         .tz_convert('US/Eastern')).tz_localize(None).date)
                else:
                    datetime_index = (pd.DatetimeIndex(pd.to_datetime(datetime_idx_list, unit='ms', utc=False)
                                                         .tz_localize('UTC')
                                                         .tz_convert('US/Eastern')).tz_localize(None)
                                                                                   .floor('T'))

                ticker_to_indicator_column = pd.MultiIndex.from_product([[ticker], [Indicator.OPEN.value, Indicator.HIGH.value, Indicator.LOW.value, Indicator.CLOSE.value, Indicator.VOLUME.value]])
                single_ticker_candle_df = pd.DataFrame(ohlcv_list, columns=ticker_to_indicator_column, index=datetime_index)

                if single_ticker_candle_df.index.duplicated().any():
                    single_ticker_candle_df = single_ticker_candle_df.loc[~single_ticker_candle_df.index.duplicated(keep='first')]

                single_ticker_candle_df = single_ticker_candle_df.reindex(datetime_range_index) # may need ffill() and bfill()
                ticker_candle_df_list.append(single_ticker_candle_df)

            logger.log_debug_msg(f'Construct ohlcv dataframe time: {time.time() - construct_dataframe_start_time}')

            if ticker_candle_df_list:
                complete_df = pd.concat(ticker_candle_df_list, axis=1)
            else:
                return None
            
            if bar_size.value.endswith('d'):
                dropped_indice = complete_df.isna().all(axis=1)[complete_df.isna().all(axis=1)].index.tolist()
                complete_df = complete_df.dropna(axis=0, how='all')
                logger.log_debug_msg(f'Nan Row Found in historical dataframe: {dropped_indice}')
            
            complete_df_ticker_list = complete_df.columns.get_level_values(0).unique()
            
            incomplete_response_ticker_list = np.setdiff1d(ticker_list, complete_df_ticker_list)
            if len(incomplete_response_ticker_list) > 0:
                logger.log_debug_msg(f'Get incomplete response in {bar_size.value} historical candle data, incomplete response ticker list: {incomplete_response_ticker_list}, full ticker list: {ticker_list}, response ticker list: {complete_df_ticker_list}, contract list: {contract_list}')
                ticker_list = [ticker for ticker in ticker_list if ticker not in incomplete_response_ticker_list]
                
            return complete_df[ticker_list]