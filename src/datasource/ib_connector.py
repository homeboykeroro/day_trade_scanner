from asyncio import AbstractEventLoop
import re
import time
from datetime import datetime, timedelta
import html
import pytz
import requests
import numpy as np
import pandas as pd
import urllib3

from model.ib.contract_info import ContractInfo
from model.ib.snapshot import Snapshot

from utils.http_util import send_async_request
from utils.collection_util import get_chunk_list
from utils.datetime_util import  US_BUSINESS_DAY, get_us_business_day, get_current_us_datetime
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
CONCAT_TICKER_CHUNK_SIZE = 300

class IBConnector:
    def __init__(self, loop: AbstractEventLoop = None) -> None:
        self.__ticker_to_contract_info_dict = {}
        self.__loop = loop
    
    def receive_brokerage_account(self):
        try:
            receive_brokerage_account_time = time.time()
            brokerage_account_response = session.get(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.ACCOUNT}', verify=False)
            logger.log_debug_msg(f'Receive brokerage account response time: {time.time() - receive_brokerage_account_time} seconds')
            brokerage_account_response.raise_for_status()
        except Exception as brokerage_account_request_exception:
            logger.log_error_msg(f'Error occurred while receiving brokerage account: {brokerage_account_request_exception}')
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

            if not reauthenticate_response.ok:
               raise requests.RequestException('Failed to reauthenticate and validate session')

            if sso_validate_response.ok:
                sso_validate_result = sso_validate_response.json()
                logger.log_debug_msg(f'SSO validation result: {sso_validate_result}', with_std_out=True)
            else:
                logger.log_debug_msg(f'SSO validation failed', with_std_out=True)
                
            reauthenticate_result = reauthenticate_response.json()
            reauthenticate_message = reauthenticate_result.get('message')
            
            if not reauthenticate_message:
                raise requests.RequestException('Failed to reauthenticate session')
        except Exception as reauthenticate_exception:
            raise reauthenticate_exception
    
    def check_auth_status(self):
        is_connection_success = True

        try:
            check_status_start_time = time.time()
            status_response = session.post(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.AUTH_STATUS}', verify=False)
            logger.log_debug_msg(f'Check authentication status response time: {time.time() - check_status_start_time} seconds')
            status_response.raise_for_status()
        except Exception as check_status_request_exception:
            logger.log_error_msg(f'Error occurred while requesting authentication status: {check_status_request_exception}')
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
    
    def get_screener_results(self, max_no_of_scanner_result: int, scanner_filter_payload: dict) -> list:
        try:
            scanner_type = scanner_filter_payload.get("type")
            scanner_request_start_time = time.time()
            scanner_response = session.post(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.RUN_SCANNER}', json=scanner_filter_payload, verify=False)
            logger.log_debug_msg(f'{scanner_type} scanner result response time: {time.time() - scanner_request_start_time} seconds')
            scanner_response.raise_for_status()
        except Exception as scanner_request_exception:
            logger.log_error_msg(f'Error occurred while requesting {scanner_type} scanner result: {scanner_request_exception}')
            raise scanner_request_exception
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
        ticker_chunk_list = get_chunk_list(ticker_list, CONCAT_TICKER_CHUNK_SIZE)

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
                                                   chunk_size=10,
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
    
    def update_contract_info(self, contract_list: list) -> None:
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
    
    def update_snapshot(self, con_id_list: list) -> None:
        if not con_id_list:
            return
        
        logger.log_debug_msg(f'Getting market cap, is shortable, shortable shares, and rebate rate data, conId list: {con_id_list}')
        snapshot_payload_list = []

        chunk_list = get_chunk_list(con_id_list, CONCAT_TICKER_CHUNK_SIZE)
        
        for chunk in chunk_list:
            get_snapshot_payload = {
                'conids': ','.join(str(con_id) for con_id in chunk),
                'fields' : SNAPSHOT_FIELD_LIST_STR
            }
            snapshot_payload_list.append(get_snapshot_payload)

        try:
            snapshot_retrieval_success = False
            
            while not snapshot_retrieval_success:
                get_contract_snapshot_start_time = time.time()
                snapshot_response_list = send_async_request(method='GET', 
                                                            endpoint=f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SNAPSHOT}', 
                                                            payload_list=snapshot_payload_list, 
                                                            loop=self.__loop)
                logger.log_debug_msg(f'Get market cap, is shortable, shortable shares, and rebate rate data response time: {time.time() - get_contract_snapshot_start_time}')
                
                for snapshot_list in snapshot_response_list:
                    incomplete_response_found = False
                    
                    for snapshot_data in snapshot_list:
                        if '_updated' not in snapshot_data:
                            logger.log_debug_msg(f'Incomplete snapshot response found: {snapshot_data}')
                            logger.log_debug_msg(f'Full snapshot response list: {snapshot_response_list}')
                            incomplete_response_found = True
                            break
                            
                    if incomplete_response_found:
                        break
                        
                if incomplete_response_found:
                    logger.log_debug_msg('Re-fetch snapshot after 1 second', with_std_out=True)
                    time.sleep(0.5)
                else:
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
        
        chunk_list = get_chunk_list(con_id_list, CONCAT_TICKER_CHUNK_SIZE)
        
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
                candle_retrieval_end_datetime = get_us_business_day(-1)
                
            if outside_rth == 'true':
                candle_retrieval_end_datetime = candle_retrieval_end_datetime.replace(hour=20, minute=0, second=0, microsecond=0)
            else:  
                candle_retrieval_end_datetime = candle_retrieval_end_datetime.replace(hour=16, minute=0, second=0, microsecond=0)

            datetime_idx_range_end_datetime = candle_retrieval_end_datetime.date()
            datetime_idx_range_start_datetime = get_us_business_day(offset_day=-subtract_day, us_date=candle_retrieval_end_datetime).date()
            request_start_time = get_us_business_day(1, candle_retrieval_end_datetime)
            period = f'{subtract_day + 1}d'
            interval = US_BUSINESS_DAY
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
                logger.log_debug_msg(f'Get incomplete response in {bar_size.value} historical candle data, incomplete response ticker list: {incomplete_response_ticker_list}, full ticker list: {ticker_list}')
                ticker_list = [ticker for ticker in ticker_list if ticker not in incomplete_response_ticker_list]
                
            return complete_df[ticker_list]