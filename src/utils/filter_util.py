import pytz
import datetime

from utils.logger import Logger

from constant.instrument import Instrument
from constant.filter.scan_code import ScanCode
from constant.filter.filter_parameter import FilterParameter

logger = Logger()

def get_scanner_filter(min_price: float = 0.35, min_percent_change: float = 10, min_usd_volume: int = 20000, max_market_cap_in_million: int = 400, additional_filter_list: list = []) -> dict:
    # Get current datetime in HK time
    hk_datetime = datetime.datetime.now()
    
    # Convert Hong Kong time to US/Eastern time
    us_eastern_timezone = pytz.timezone('US/Eastern')
    hong_kong_timezone = pytz.timezone('Asia/Hong_Kong')
    hk_datetime = hong_kong_timezone.localize(hk_datetime)
    us_time = hk_datetime.astimezone(us_eastern_timezone)
    
    # Define trading hours in US/Eastern time
    normal_trading_hour_end_time = datetime.time(16, 0, 0)
    after_hours_trading_hour_end_time = datetime.time(20, 0, 0)
    
    scan_code_param = ScanCode.TOP_GAINERS.value
    min_percent_change_param = FilterParameter.MIN_PERCENT_CHANGE.value
    
    if normal_trading_hour_end_time <= us_time.time() < after_hours_trading_hour_end_time:
        scan_code_param = ScanCode.TOP_GAINERS_IN_AFTER_HOURS.value
        min_percent_change_param = FilterParameter.MIN_PERCENT_CHANGE_AFTER_HOURS.value
    
    scanner_filter = {
        # afterHoursChangePercAbove
        'instrument': Instrument.STOCKS.value,
        'type': scan_code_param,
        'location': 'STK.US.MAJOR',
        'filter': [
          {
            'code': FilterParameter.MIN_PRICE.value, 
            'value': min_price 
          },
          {
            'code': min_percent_change_param,
            'value': min_percent_change
          },
          {
            'code': FilterParameter.MIN_USD_VOLUME.value,
            'value': min_usd_volume
          },
          {
            'code': FilterParameter.MAX_MARKET_CAP.value,
            'value': max_market_cap_in_million
          }
        ]
    }
    
    if len(additional_filter_list) > 0:
        for additional_filter in additional_filter_list:
            if 'code' in additional_filter and 'value' in additional_filter:                
                scanner_filter['filter'].append(additional_filter_list)
    
    return scanner_filter
