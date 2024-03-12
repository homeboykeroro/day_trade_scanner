import datetime
import pandas as pd
import numpy as np
import pytz
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

US_EASTERN_TIMEZONE = pytz.timezone('US/Eastern')
HONG_KONG_TIMEZONE = pytz.timezone('Asia/Hong_Kong')
PRE_MARKET_START_DATETIME = datetime.datetime.now().astimezone(US_EASTERN_TIMEZONE).replace(hour=4, minute=0, second=0, microsecond=0)

US_BUSINESS_DAY = CustomBusinessDay(calendar=USFederalHolidayCalendar())
US_FEDERAL_HOLIDAYS = US_BUSINESS_DAY.calendar.holidays

def convert_into_human_readable_time(pop_up_datetime):
    pop_up_hour = pd.to_datetime(pop_up_datetime).hour
    pop_up_minute = pd.to_datetime(pop_up_datetime).minute
    display_hour = ('0' + str(pop_up_hour)) if pop_up_hour < 10 else pop_up_hour
    display_minute = ('0' + str(pop_up_minute)) if pop_up_minute < 10 else pop_up_minute
    return f'{display_hour}:{display_minute}'

def convert_into_read_out_time(pop_up_datetime):
    pop_up_hour = pd.to_datetime(pop_up_datetime).hour
    pop_up_minute = pd.to_datetime(pop_up_datetime).minute
    
    read_out_time = f'{pop_up_hour} {pop_up_minute}' if (pop_up_minute > 0) else f'{pop_up_hour} o clock' 
    return read_out_time

def is_within_trading_day_and_hours() -> bool:
    us_current_datetime = get_current_us_datetime()
    pre_market_trading_hour_start_time = datetime.time(4, 0, 0)
    after_hours_trading_hour_end_time = datetime.time(20, 0, 0)
    
    # Check if current datetime is a US federal holiday
    is_us_federal_holiday = us_current_datetime.date() in np.isin(us_current_datetime.date(), US_FEDERAL_HOLIDAYS)

    if not is_us_federal_holiday:
        if us_current_datetime.weekday() > 5:
            return False

        if pre_market_trading_hour_start_time <= us_current_datetime.time() <= after_hours_trading_hour_end_time:
            return True
        else:
            return False
    else:
        return False
    
def get_current_us_datetime() -> datetime:
    return datetime.datetime.now().astimezone(US_EASTERN_TIMEZONE)

def get_us_business_day(offset_day: int, us_date: datetime.datetime = None) -> datetime.datetime:
    if not us_date: 
        us_business_day = get_current_us_datetime()
    else:
        us_business_day = us_date
        
    return us_business_day + (offset_day * US_BUSINESS_DAY)

def check_if_us_business_day(us_date: datetime.datetime) -> bool:
    us_business_day = get_us_business_day(0, us_date)
    
    return us_business_day == us_date

def get_last_us_business_day(year, month):
    # Get the last day of the month
    last_day = pd.Timestamp(year, month, 1) + pd.offsets.MonthEnd(1)
    
    # If the last day of the month is not a US business day, roll it forward to the next business day
    if US_BUSINESS_DAY.rollforward(last_day) != last_day:
        last_day -= US_BUSINESS_DAY
    
    return last_day

def convert_us_to_hk_datetime(us_datetime: datetime.datetime) -> datetime.datetime:
    return us_datetime.astimezone(HONG_KONG_TIMEZONE)

def get_offsetted_hit_scanner_datetime(indice: pd.DatetimeIndex, hit_scanner_datetime: pd.Timestamp, positive_offset: int, negative_offset: int):
    datetime_idx_list = indice.tolist()
    
    if positive_offset is None and negative_offset is None:
        return datetime_idx_list[0], datetime_idx_list[-1]
    
    hit_scanner_datetime_idx_positiion = datetime_idx_list.index(hit_scanner_datetime)
    negative_offsetted_idx_position = hit_scanner_datetime_idx_positiion - negative_offset
    positive_offsetted_idx_position = hit_scanner_datetime_idx_positiion + positive_offset
    
    candle_start_range = datetime_idx_list[0] if negative_offsetted_idx_position <= 0 else datetime_idx_list[negative_offsetted_idx_position]
    candle_end_range = datetime_idx_list[-1] if positive_offsetted_idx_position >= len(datetime_idx_list) else datetime_idx_list[positive_offsetted_idx_position]
    
    return candle_start_range, candle_end_range