import datetime
import pandas as pd
import pytz

US_EASTERN_TIMEZONE = pytz.timezone('US/Eastern')
PRE_MARKET_START_DATETIME = datetime.datetime.now().astimezone(US_EASTERN_TIMEZONE).replace(hour=4, minute=0, second=0, microsecond=0)

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
    
    if us_current_datetime.weekday() > 5:
        return False
        
    if pre_market_trading_hour_start_time <= us_current_datetime.time() <= after_hours_trading_hour_end_time:
        return True
    else:
        return False
    
def get_current_us_datetime() -> datetime:
    return datetime.datetime.now().astimezone(US_EASTERN_TIMEZONE)