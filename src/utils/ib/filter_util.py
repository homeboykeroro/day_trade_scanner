import pytz
import datetime

from constant.scanner.ib.instrument import Instrument
from constant.scanner.ib.scan_code import ScanCode
from constant.scanner.scanner_target import ScannerTarget
from constant.scanner.ib.filter_parameter import FilterParameter

def get_finviz_scanner_filter(scan_target: ScannerTarget):
  if scan_target == ScannerTarget.TOP_GAINER:
    scan_type = 'ta_topgainers'
  elif scan_target == ScannerTarget.TOP_LOSER:
    scan_type = 'ta_toplosers'
    
  scanner_filter = {
    's': scan_type
  }

  return scanner_filter

def get_ib_scanner_filter(instrument: Instrument = Instrument.STOCKS,
                          scan_target: ScannerTarget = ScannerTarget.TOP_GAINER, 
                          min_price: float = 0.3, 
                          percent_change_param: float = 10, 
                          min_usd_volume: int = 20000, 
                          max_market_cap: int = 1e6 * 500, 
                          additional_filter_list: list = []) -> dict:
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
    
    if scan_target == ScannerTarget.TOP_GAINER:
      scan_code_param = ScanCode.TOP_GAINERS.value
      percent_change_param_code = FilterParameter.MIN_PERCENT_CHANGE.value
    elif scan_target == ScannerTarget.TOP_LOSER:
      scan_code_param = ScanCode.TOP_LOSERS.value
      percent_change_param_code = FilterParameter.MAX_PERCENT_CHANGE.value
    
    if normal_trading_hour_end_time <= us_time.time() < after_hours_trading_hour_end_time:
      if scan_target == ScannerTarget.TOP_GAINER:
        scan_code_param = ScanCode.TOP_GAINERS_IN_AFTER_HOURS.value
        percent_change_param_code = FilterParameter.MIN_PERCENT_CHANGE_AFTER_HOURS.value
      elif scan_target == ScannerTarget.TOP_LOSER:
        scan_code_param = ScanCode.TOP_LOSER_IN_AFTER_HOURS.value
        percent_change_param_code = FilterParameter.MAX_PERCENT_CHANGE_AFTER_HOURS.value
      
    scanner_filter = {
        # afterHoursChangePercAbove
        'instrument': instrument.value,
        'type': scan_code_param,
        'location': 'STK.US.MAJOR',
        'filter': [
          {
            'code': FilterParameter.MIN_PRICE.value, 
            'value': min_price 
          },
          {
            'code': percent_change_param_code,
            'value': percent_change_param
          },
          {
            'code': FilterParameter.MIN_USD_VOLUME.value,
            'value': min_usd_volume
          },
          {
            'code': FilterParameter.MAX_MARKET_CAP.value,
            'value': max_market_cap / 1e6
          }
        ]
    }
    
    if len(additional_filter_list) > 0:
        for additional_filter in additional_filter_list:
            if 'code' in additional_filter and 'value' in additional_filter:                
                scanner_filter['filter'].append(additional_filter_list)
    
    return scanner_filter
