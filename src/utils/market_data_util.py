import re
import math
import time
import datetime
import html
import requests
import numpy as np
import pandas as pd
import urllib3

from model.contract_info import ContractInfo

from utils.datetime_util import US_EASTERN_TIMEZONE, PRE_MARKET_START_DATETIME, get_current_us_datetime
from utils.http_util import send_async_request
from utils.filter_util import get_scanner_filter
from utils.logger import Logger

from constant.client_portal_api_endpoint import ClientPortalApiEndpoint
from constant.candle.bar_size import BarSize
from constant.candle.candle_colour import CandleColour
from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator

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
SNAPSHOT_FIELD_LIST = [55, 7221, 7051, 7289, 7644, 7636, 7637]
SNAPSHOT_FIELD_LIST_PARAM = ','.join(str(field) for field in SNAPSHOT_FIELD_LIST)
CANDLE_MINUTE_RETRIEVAL_START_STR = (datetime.datetime.now()
                                                  .astimezone(US_EASTERN_TIMEZONE)
                                                  .replace(hour=4, minute=0, second=0, microsecond=0, tzinfo=None)
                                                  .strftime('%Y%m%d-%H:%M:%S'))

def check_auth_status():
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

def get_scanner_result(max_no_of_scanner_result: int) -> dict:
    try:
        scanner_request_start_time = time.time()
        run_scanner_payload = get_scanner_filter()
        scanner_response = session.post(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.RUN_SCANNER}', json=run_scanner_payload, verify=False)
        logger.log_debug_msg(f'Scanner result response time: {time.time() - scanner_request_start_time} seconds')
        scanner_response.raise_for_status()
    except Exception as scanner_request_exception:
        logger.log_error_msg(f'Error occurred while requesting scanner result: {scanner_request_exception}')
        raise scanner_request_exception
    else:
        logger.log_debug_msg(f'Scanner full result json: {scanner_response.json()}')
        logger.log_debug_msg(f'Scanner full result size: {scanner_response.json()}')
        logger.log_debug_msg(f'Maximum scanner result size: {max_no_of_scanner_result}')
        scanner_result = scanner_response.json()['contracts']
        scanner_result_without_otc_stock = []
        
        for result in scanner_result:
            if 'symbol' in result:
                symbol = result['symbol']
            
                if re.match('^[a-zA-Z]{1,4}$', symbol): 
                    scanner_result_without_otc_stock.append(result)
                else:
                    logger.log_debug_msg(f'Exclude {symbol} from scanner result')
            else:
                logger.log_debug_msg(f'Exclude unknown contract from scanner result, {result}', with_std_out=True)
    
        return scanner_result_without_otc_stock[:max_no_of_scanner_result]

def get_contract_minute_candle_data(contract_list: list, ticker_to_previous_day_data_dict: dict, ticker_to_contract_dict: dict) -> pd.DataFrame:
    result_dict = {}

    us_current_datetime = get_current_us_datetime().replace(microsecond=0, second=0)
    historical_data_interval_in_minute = int((us_current_datetime - PRE_MARKET_START_DATETIME).total_seconds() / 60)
    logger.log_debug_msg(f'Historical candle data retrieval period: {historical_data_interval_in_minute} minutes')
    
    if historical_data_interval_in_minute < 1:
        logger.log_debug_msg('Historical candle data retrieval retrieval period is less than 1 minute', with_std_out=True)
        return result_dict
        
    snapshot_data_con_id_list = []
    previous_day_data_payload_list = []
    one_minute_candle_payload_list = []
    five_minute_candle_payload_list = []
    
    previous_day_data_period = '2d' if (datetime.time(4, 0, 0) <= us_current_datetime.time() < datetime.time(20, 0, 0)) else '1d'
    logger.log_debug_msg(f'Get previous day data period: {previous_day_data_period}')
    
    for contract in contract_list:
        con_id = contract['con_id']
        ticker_symbol = contract['symbol']
        
        if ticker_symbol not in ticker_to_contract_dict:
            snapshot_data_con_id_list.append(con_id)
        
        if ticker_symbol not in ticker_to_previous_day_data_dict:
            previous_day_data_payload = {
                'conid': str(con_id),
                'period': previous_day_data_period,
                'bar': BarSize.ONE_DAY.value,
                'outsideRth': 'true'
            }  
            previous_day_data_payload_list.append(previous_day_data_payload)  
            
        one_minute_candle_payload = {
            'conid': str(con_id),
            'period': f'{historical_data_interval_in_minute}min',
            'bar': BarSize.ONE_MINUTE.value,
            'outsideRth': 'true'
        }
            
        five_minute_candle_payload = {
            'conid': str(con_id),
            'period': f'{historical_data_interval_in_minute}min',
            'bar': BarSize.FIVE_MINUTE.value,
            'outsideRth': 'true'
        }
            
        one_minute_candle_payload_list.append(one_minute_candle_payload)
        five_minute_candle_payload_list.append(five_minute_candle_payload)
   
    if len(snapshot_data_con_id_list) > 0:
        set_contract_snapshot_data(snapshot_data_con_id_list, ticker_to_contract_dict)
        set_sec_def(snapshot_data_con_id_list, ticker_to_contract_dict)
            
    if len(previous_day_data_payload_list) > 0:
        set_previous_day_data(previous_day_data_payload_list, ticker_to_previous_day_data_dict)
    
    candle_start_datetime = PRE_MARKET_START_DATETIME
    candle_end_datetime = us_current_datetime
    
    result_dict[BarSize.ONE_MINUTE] = get_historical_candle_data(candle_start_datetime, candle_end_datetime, BarSize.ONE_MINUTE.value, one_minute_candle_payload_list, ticker_to_previous_day_data_dict)
    result_dict[BarSize.FIVE_MINUTE] = get_historical_candle_data(candle_start_datetime, candle_end_datetime, BarSize.FIVE_MINUTE.value, five_minute_candle_payload_list, ticker_to_previous_day_data_dict)
            
    return result_dict

def set_contract_snapshot_data(con_id_list: list, ticker_to_contract_dict: dict) -> None:
    logger.log_debug_msg(f'Getting contract snapshot information, conId list: {con_id_list}')
    
    get_snapshot_payload = {
        'conids': ','.join(str(con_id) for con_id in con_id_list),
        'fields' : SNAPSHOT_FIELD_LIST_PARAM
    }
    
    try:
        get_contract_snapshot_start_time = time.time()
        snapshot_response = session.get(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SNAPSHOT}', params=get_snapshot_payload)
        logger.log_debug_msg(f'Get contract snapshot information time: {time.time() - get_contract_snapshot_start_time}')
        snapshot_response.raise_for_status()
    except Exception as snapshot_request_exception:
        logger.log_error_msg(f'Error occurred while requesting snapshot information, Cause: {snapshot_request_exception}')
        raise snapshot_request_exception
    else:
        snapshot_result = snapshot_response.json()
    
        for snapshot_data in snapshot_result:
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
                logger.log_debug_msg(f'No ticker symbol data is available for {snapshot_data}')
            
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
                
            contract_info = ContractInfo(con_id, symbol, exchange, company_name, None, market_cap, shortable, shortable_shares, rebate_rate)
            ticker_to_contract_dict[symbol] = contract_info

def set_sec_def(con_id_list: list, ticker_to_contract_dict: dict):
    logger.log_debug_msg(f'Getting security definitions, conId list: {con_id_list}')
    
    get_snapshot_payload = {
        'conids': ','.join(str(con_id) for con_id in con_id_list)
    }
    
    try:
        get_security_definitions_start_time = time.time()
        security_definitions_response = session.get(f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SECURITY_DEFINITIONS}', params=get_snapshot_payload)
        logger.log_debug_msg(f'Get security definitions time: {time.time() - get_security_definitions_start_time}')
        security_definitions_response.raise_for_status()
    except Exception as snapshot_request_exception:
        logger.log_error_msg(f'Error occurred while requesting security definitions, Cause: {snapshot_request_exception}')
        raise snapshot_request_exception
    else:
        security_definitions_result = security_definitions_response.json()['secdef']
        
        for sec_df in security_definitions_result:
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
            
            if ticker in ticker_to_contract_dict:
                ticker_to_contract_dict[ticker].sector = f'{sector_group}, {group}'
            else:
                logger.log_debug_msg(f'{ticker} not exist in ticker_to_contract_dict')

def set_previous_day_data(previous_day_data_payload_list: list, ticker_to_previous_day_data_dict: dict) -> None:
    logger.log_debug_msg(f'Getting previous day data, paylaod list: {previous_day_data_payload_list}')
    
    try:
        get_previous_close_start_time = time.time()
        previous_close_candle_response_list = send_async_request('GET', f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.MARKET_DATA_HISTORY}', previous_day_data_payload_list, 5)
        logger.log_debug_msg(f'Get previous day data time: {time.time() - get_previous_close_start_time}')
    except Exception as scanner_request_exception:
        logger.log_error_msg(f'Error occurred while requesting previous day data: {scanner_request_exception}')
        raise scanner_request_exception
    else:
        for previous_close_data_response in previous_close_candle_response_list:
            if 'error' in previous_close_data_response:
                logger.log_debug_msg(f'Exclude invalid previous close data response: {previous_close_data_response}')
                continue
            
            ticker = previous_close_data_response['symbol']
            ohlcv_list = previous_close_data_response['data']

            open = ohlcv_list[0]['o']
            high = ohlcv_list[0]['h']
            low = ohlcv_list[0]['l']
            close = ohlcv_list[0]['c']
            volume = int(ohlcv_list[0]['v'] * 100)
            dt = pd.to_datetime(ohlcv_list[0]['t'], unit='ms').tz_localize('UTC').tz_convert('US/Eastern')

            ticker_to_previous_day_data_dict[ticker] = {
                'open': open,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
                'dt': dt
            }
    
def get_historical_candle_data(start_datetime: datetime, end_datetime: datetime, freq: str, minute_candle_payload_list: list, ticker_to_previous_day_data_dict: dict):
    try:
        logger.log_debug_msg(f'Getting historical minute candle data, paylaod list: {minute_candle_payload_list}')
        get_one_minute_candle_start_time = time.time()
        minute_candle_response_list = send_async_request('GET', f'{ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.MARKET_DATA_HISTORY}', minute_candle_payload_list, 5)
        logger.log_debug_msg(f'Get historical candle data time: {time.time() - get_one_minute_candle_start_time}')
    except Exception as historical_data_request_exception:
        logger.log_error_msg(f'An error occurred while requesting historical data, Cause: {historical_data_request_exception}')
        raise historical_data_request_exception
    else:
        return construct_complete_dataframe(start_datetime, end_datetime, freq, minute_candle_response_list, ticker_to_previous_day_data_dict)
                   
def construct_complete_dataframe(start_datetime: datetime, end_datetime: datetime, freq: str, historical_data_response_list: list, ticker_to_previous_day_data_dict: dict) -> pd.DataFrame:
    datetime_range_index = pd.date_range(start=start_datetime.replace(tzinfo=None), end=end_datetime.replace(tzinfo=None), freq=freq)
    
    construct_dataframe_start_time = time.time()
    ticker_candle_df_list = []
            
    for historical_data in historical_data_response_list:
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
        
        # logger.log_debug_msg(f'{ticker} Candle US time list: {datetime_idx_list}')
        datetime_index = (pd.DatetimeIndex(pd.to_datetime(datetime_idx_list, unit='ms', utc=False)
                                             .tz_localize('UTC')
                                             .tz_convert('US/Eastern')).tz_localize(None)
                                                                       .round(freq='S'))

        ticker_to_indicator_column = pd.MultiIndex.from_product([[ticker], [Indicator.OPEN.value, Indicator.HIGH.value, Indicator.LOW.value, Indicator.CLOSE.value, Indicator.VOLUME.value]])
        single_ticker_candle_df = pd.DataFrame(ohlcv_list, columns=ticker_to_indicator_column, index=datetime_index)
        
        if single_ticker_candle_df.index.duplicated().any():
            single_ticker_candle_df = single_ticker_candle_df.loc[~single_ticker_candle_df.index.duplicated(keep='first')]
        
        # with pd.option_context('display.max_rows', None,
        #    'display.max_columns', None,
        #    'display.precision', 3,
        #    ):
        #     logger.log_debug_msg(f'Individual ticker original dataframe: {single_ticker_candle_df}', with_log_file=True, with_std_out=False)
        
        single_ticker_candle_df = single_ticker_candle_df.reindex(datetime_range_index) # may need ffill() and bfill()
        # with pd.option_context('display.max_rows', None,
        #    'display.max_columns', None,
        #    'display.precision', 3,
        #    ):
        #     logger.log_debug_msg(f'Individual ticker reindexed dataframe: {single_ticker_candle_df}', with_log_file=True, with_std_out=False)
        
        ticker_candle_df_list.append(single_ticker_candle_df)
                
    all_ticker_candle_df = pd.concat(ticker_candle_df_list, axis=1)
    ticker_column_name_list = list(all_ticker_candle_df.columns.get_level_values(0).unique())
    previous_close_list = [[float(ticker_to_previous_day_data_dict[ticker]['close']) for ticker in ticker_column_name_list]]
    previous_close_df = pd.DataFrame(np.repeat(previous_close_list, 
                                                    len(all_ticker_candle_df), 
                                                    axis=0),
                                     columns=pd.MultiIndex.from_product([ticker_column_name_list, [CustomisedIndicator.PREVIOUS_CLOSE.value]]),
                                     index=all_ticker_candle_df.index)
        
    open_df = all_ticker_candle_df.loc[:, idx[:, Indicator.OPEN.value]].rename(columns={Indicator.OPEN.value: RuntimeIndicator.COMPARE.value})
    high_df = all_ticker_candle_df.loc[:, idx[:, Indicator.HIGH.value]].rename(columns={Indicator.HIGH.value: RuntimeIndicator.COMPARE.value})
    low_df = all_ticker_candle_df.loc[:, idx[:, Indicator.LOW.value]].rename(columns={Indicator.LOW.value: RuntimeIndicator.COMPARE.value})
    close_df = all_ticker_candle_df.loc[:, idx[:, Indicator.CLOSE.value]].rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
    vol_df = all_ticker_candle_df.loc[:, idx[:, Indicator.VOLUME.value]]

    previous_close_pct_df = (((close_df.sub(previous_close_df.values))
                                       .div(previous_close_df.values))
                                       .mul(100)).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.PREVIOUS_CLOSE_CHANGE.value})
                
    close_pct_df = close_df.pct_change().mul(100).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CLOSE_CHANGE.value})
    close_pct_df.iloc[0] = previous_close_pct_df.iloc[0].values
                
    flat_candle_df = (open_df == close_df).replace({True: CandleColour.GREY.value, False: np.nan})
    green_candle_df = (close_df > open_df).replace({True: CandleColour.GREEN.value, False: np.nan})
    red_candle_df = (close_df < open_df).replace({True: CandleColour.RED.value, False: np.nan})
    colour_df = ((flat_candle_df.fillna(green_candle_df))
                                .fillna(red_candle_df)
                                .rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CANDLE_COLOUR.value}))
                
    vol_cumsum_df = vol_df.cumsum().rename(columns={Indicator.VOLUME.value: CustomisedIndicator.TOTAL_VOLUME.value})
    # vol_20_ma_df = vol_df.rolling(window=20, min_periods=1).mean().rename(columns={Indicator.VOLUME.value: CustomisedIndicator.MA_20_VOLUME.value})
    # vol_50_ma_df = vol_df.rolling(window=50, min_periods=1).mean().rename(columns={Indicator.VOLUME.value: CustomisedIndicator.MA_50_VOLUME.value})

    typical_price_df = ((high_df.add(low_df.values)
                                .add(close_df.values))
                                .div(3))
    tpv_cumsum_df = typical_price_df.mul(vol_df.values).cumsum()
    # vwap_df = tpv_cumsum_df.div(vol_cumsum_df.values).rename(columns={Indicator.HIGH.value: CustomisedIndicator.VWAP.value})

    close_above_open_boolean_df = (close_df > open_df)
    high_low_diff_df = high_df.sub(low_df.values)
    close_above_open_upper_body_df = close_df.where(close_above_open_boolean_df.values)
    open_above_close_upper_body_df = open_df.where((~close_above_open_boolean_df).values)
    candle_upper_body_df = close_above_open_upper_body_df.fillna(open_above_close_upper_body_df).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CANDLE_UPPER_BODY.value})

    close_above_open_lower_body_df = open_df.where(close_above_open_boolean_df.values)
    open_above_close_lower_body_df = close_df.where((~close_above_open_boolean_df).values)
    candle_lower_body_df = close_above_open_lower_body_df.fillna(open_above_close_lower_body_df).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CANDLE_LOWER_BODY.value})

    body_diff_df = candle_upper_body_df.sub(candle_lower_body_df.values)
    marubozu_ratio_df = (body_diff_df.div(high_low_diff_df.values)).mul(100).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.MARUBOZU_RATIO.value})

    complete_df = pd.concat([all_ticker_candle_df, 
                        close_pct_df,
                        previous_close_df,
                        previous_close_pct_df,
                        vol_cumsum_df,
                        colour_df,
                        marubozu_ratio_df,
                        candle_upper_body_df,
                        candle_lower_body_df], axis=1)
                        
    logger.log_debug_msg(f'Construct DataFrame time: {time.time() - construct_dataframe_start_time}')
                
    # with pd.option_context('display.max_rows', None,
    #        'display.max_columns', None,
    #        'display.precision', 3,
    #        ):
    #     logger.log_debug_msg(f'Candle dataframe: {all_ticker_candle_df}', with_log_file=True, with_std_out=False)
        
    return complete_df
