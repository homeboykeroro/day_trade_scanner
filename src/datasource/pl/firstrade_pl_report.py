import calendar
from datetime import datetime, timedelta
from collections import OrderedDict
import re
import pandas as pd
import pdfquery

from module.discord_chatbot_client import DiscordChatBotClient

from sql.sqlite_connector import SqliteConnector

from datasource.ib_connector import IBConnector
from datasource.pl.pl_report import PLReport

from model.pl.trade_profit_and_loss import TradeProfitAndLoss
from model.pl.interest_profit import InterestProfit

from constant.broker import Broker

from utils.logger import Logger
from utils.datetime_util import get_current_us_datetime, get_last_us_business_day

logger = Logger()
idx = pd.IndexSlice

# Trade Fields
SYMBOL = 'Symbol'
TRADE_DATE = 'TradeDate'
QUANTITY = 'Quantity'
ACTION = 'Action'
TRADE_PRICE = 'Price'
AMOUNT = 'Amount'
ORDER_TYPE = 'Action'

DAILY_REALISED_PL = 'DAILY_REALISED_PL'
WEEKLY_REALISED_PL = 'WEEKLY_REALISED_PL'
MONTH_TO_DATE_REALISED_PL = 'MONTH_TO_DATE_REALISED_PL'
YEAR_TO_DATE_REALISED_PL = 'YEAR_TO_DATE_REALISED_PL'
MONTHLY_REALISED_PL = 'MONTHLY_REALISED_PL'
YEARLY_REALISED_PL = 'YEARLY_REALISED_PL'
MTD_REALISED_PL = 'MTD_REALISED_PL'
YTD_REALISED_PL = 'YTD_REALISED_PL'
DAY_TRADE_SUMMARY = 'DAY_TRADE_SUMMARY'
SWING_TRADE_SUMMARY = 'SWING_TRADE_SUMMARY'

class FirstradePLReport(PLReport):
    def __init__(self, ib_connector: IBConnector, sqlite_connector: SqliteConnector, discord_client: DiscordChatBotClient) -> None:
        super().__init__(sqlite_connector, discord_client)
        self.__ib_connector = ib_connector
        
    @property
    def account_summary_data(self):
        return self.__account_summary_data
    
    @account_summary_data.setter
    def account_summary_data(self, account_summary_data):
        self.__account_summary_data = account_summary_data
    
    def __normalise_column_dtype(self, src_df: pd.DataFrame) -> pd.DataFrame:
        src_df[SYMBOL] = src_df[SYMBOL].str.strip()
        src_df[TRADE_DATE] = pd.to_datetime(src_df[TRADE_DATE], errors='coerce')
        src_df[QUANTITY] = src_df[QUANTITY].astype(int).abs()
        src_df[TRADE_PRICE] = src_df[TRADE_PRICE].astype(float)
        src_df[AMOUNT] = src_df[AMOUNT].astype(float).abs()

        return src_df
    
    def __get_interest_message_dict(self, src_df: pd.DataFrame) -> dict:
        current_date = get_current_us_datetime()
        last_us_business_day = get_last_us_business_day(current_date.year, current_date.month)
        is_on_or_after_settle_date = current_date.date() >= last_us_business_day.date()
        monthly_interest_dict = {}
        
        interest_fee_boolean_df = (src_df[ACTION] == 'Interest')
        interest_df = src_df[interest_fee_boolean_df].reset_index(drop=True)
    
        for row in range(len(interest_df)):
            interest_report_date = interest_df.loc[row, TRADE_DATE]
            report_year = interest_report_date.year
            report_month = interest_report_date.month
            total_interest = interest_df.loc[row, AMOUNT]
            
            if report_year < current_date.year or (report_year == current_date.year and report_month < current_date.month):
                if (report_year, report_month) not in monthly_interest_dict:
                    monthly_interest_dict[(report_year, report_month)] = 0

                monthly_interest_dict[(report_year, report_month)] += total_interest
            elif is_on_or_after_settle_date:
                if (report_year, report_month) not in monthly_interest_dict:
                    monthly_interest_dict[(report_year, report_month)] = 0
                
                monthly_interest_dict[(report_year, report_month)] += total_interest
        
        interest_message_list = []
        for year_month, interest in monthly_interest_dict.items():
            last_day_of_month = (calendar.monthrange(year_month[0], year_month[1]))[1]
            
            interest_message = InterestProfit(settle_date=datetime(year_month[0], year_month[1], last_day_of_month),
                                              interest_value=interest,
                                              paid_by=Broker.FIRSTRADE)
            interest_message_list.append(interest_message)

        return interest_message_list

    def get_trade_summary(self, src_df: pd.DataFrame):
        non_stock_trade_history_boolean_df = src_df[SYMBOL] == ''
        valid_ticker_boolean_df = src_df[SYMBOL].str.contains('^[A-Z]{0,4}$', regex=True, na=False)
        stock_trade_history_df = src_df[(~non_stock_trade_history_boolean_df) & valid_ticker_boolean_df]
        buy_order_boolean_df = (stock_trade_history_df[ORDER_TYPE] == 'BUY')
        sell_order_boolean_df = (stock_trade_history_df[ORDER_TYPE] == 'SELL')
        buy_order_df = stock_trade_history_df[buy_order_boolean_df].groupby([SYMBOL, TRADE_DATE]).agg({QUANTITY:'sum', AMOUNT: 'sum'})
        sell_order_df = stock_trade_history_df[sell_order_boolean_df].groupby([SYMBOL, TRADE_DATE]).agg({QUANTITY:'sum', AMOUNT: 'sum'})
        
        ticker_to_open_position_datetime = list(buy_order_df.sort_index().index)
        ticker_to_close_position_datetime = list(sell_order_df.sort_index().index)
        
        ticker_to_buy_datetime_list_dict = {}
        for symbol, date in ticker_to_open_position_datetime:
            if symbol not in ticker_to_buy_datetime_list_dict:
                ticker_to_buy_datetime_list_dict[symbol] = []
            ticker_to_buy_datetime_list_dict[symbol].append(date)
            
        ticker_to_sell_datetime_list_dict = {}
        for symbol, date in ticker_to_close_position_datetime:
            if symbol not in ticker_to_sell_datetime_list_dict:
                ticker_to_sell_datetime_list_dict[symbol] = []
            ticker_to_sell_datetime_list_dict[symbol].append(date)
        
        ticker_to_trade_history_datetime_list_dict = {}
        
        for symbol, ticker_to_buy_datetime_list in ticker_to_buy_datetime_list_dict.items():
            close_position_datetime_list = ticker_to_sell_datetime_list_dict.get(symbol) 
            
            if close_position_datetime_list:
                datetime_list = list(set(ticker_to_buy_datetime_list + close_position_datetime_list))
                datetime_list.sort()
                ticker_to_trade_history_datetime_list_dict[symbol] = datetime_list
        
        # ticker_list = list({ticker for ticker, _ in ticker_to_trade_history_datetime_list_dict.items()})
        # con_id_list = self.__ib_connector.get_security_by_tickers(ticker_list)
        # self.__ib_connector.update_contract_info(con_id_list)
        # ticker_to_contract_dict = self.__ib_connector.get_ticker_to_contract_dict()
        # ticker_to_contract_dict = {}
        
        ordered_src_df = stock_trade_history_df.sort_values(by=[SYMBOL, TRADE_DATE, ORDER_TYPE])
        
        date_to_ticker_transaction_dict = {}
        for ticker, trade_history_datetime_list in ticker_to_trade_history_datetime_list_dict.items():
            accumulated_buy_cost = 0
            accumulated_buy_share = 0
            accumulated_sell_market_value = 0
            accumulated_sell_share = 0
            
            for date in trade_history_datetime_list:
                pl_message_list = []
                trade_boolean_df = ((ordered_src_df[TRADE_DATE] == date) & (ordered_src_df[SYMBOL] == ticker))
                trade_df = ordered_src_df[trade_boolean_df].reset_index(drop=True)
                close_position = False
                for row in range(len(trade_df)):
                    order_type = trade_df.loc[row, ORDER_TYPE]
                    share = trade_df.loc[row, QUANTITY]
                    trade_datetime = trade_df.loc[row, TRADE_DATE]
                    trade_price = trade_df.loc[row, TRADE_PRICE]

                    if order_type == 'BUY':
                        accumulated_buy_share += share
                        accumulated_buy_cost += (trade_price * share)
                        latest_fillied_buy_order_datetime = trade_datetime # minor bug fix, need to change to earilest load share datetime
                    else:
                        accumulated_sell_share += share
                        accumulated_sell_market_value += (trade_price * share)
                        latest_fillied_sell_order_datetime = trade_datetime
                        
                        if row + 1 < len(trade_df):
                            if trade_df.loc[row + 1, ORDER_TYPE] == 'BUY':
                                close_position = True
                        else:
                            close_position = True

                    # Close partial/ all positions
                    if close_position:
                        if accumulated_buy_share > 0:
                            avg_entry_price = accumulated_buy_cost / accumulated_buy_share
                            avg_exit_price = accumulated_sell_market_value / accumulated_sell_share

                            adjusted_cost = avg_entry_price * accumulated_sell_share
                            realised_pl = accumulated_sell_market_value - adjusted_cost
                            realised_pl_percent = round(((realised_pl/ adjusted_cost) * 100), 3)

                            pl_message_param_dict = dict(ticker=ticker, 
                                                         acquired_date=latest_fillied_buy_order_datetime, sold_date=latest_fillied_sell_order_datetime,
                                                         realised_pl=round(realised_pl), realised_pl_percent=realised_pl_percent,
                                                         accumulated_cost=accumulated_buy_cost, adjusted_cost=adjusted_cost, market_value=round(accumulated_sell_market_value),
                                                         accumulated_shares=accumulated_buy_share, sell_quantity=accumulated_sell_share, remaining_positions=(accumulated_buy_share - accumulated_sell_share),
                                                         avg_entry_price=round(avg_entry_price, 3), avg_exit_price=round(avg_exit_price, 3), 
                                                         trading_platform=Broker.FIRSTRADE,
                                                         contract_info={})
                            pl_message = TradeProfitAndLoss(**pl_message_param_dict)
                            pl_message_list.append(pl_message)

                            accumulated_buy_share = accumulated_buy_share - accumulated_sell_share

                            if accumulated_buy_share > 0:
                                accumulated_buy_cost = accumulated_buy_cost - adjusted_cost
                            elif accumulated_buy_share == 0:
                                accumulated_buy_cost = 0

                            close_position = False
                            accumulated_sell_share = 0
                            accumulated_sell_market_value = 0
                        else:
                            logger.log_debug_msg(f'Invalid record of trade, ticker: {ticker}, trade date: {date}, order type: {order_type}, bought share: {accumulated_buy_share}', with_std_out=True)
                
                if date not in date_to_ticker_transaction_dict:
                    date_to_ticker_transaction_dict[date] = {}
                
                if pl_message_list:
                    date_to_ticker_transaction_dict[date][ticker] = pl_message_list
        
        ordered_close_position_date_to_trade_summary_list_dict = OrderedDict(sorted(date_to_ticker_transaction_dict.items(), key=lambda t: t[0]))
        close_position_date_to_trade_summary_list_dict = {}
        
        for date, ticker_to_trade_info_list in ordered_close_position_date_to_trade_summary_list_dict.items():
            if ticker_to_trade_info_list:
                close_position_date_to_trade_summary_list_dict[date] = ticker_to_trade_info_list
        
        daily_pl_dict = {}
        day_trade_summary_list = []
        swing_trade_summary_list = []
        for date, ticker_to_trade_info_list_dict in close_position_date_to_trade_summary_list_dict.items():
            daily_pl = 0
            for ticker, trade_info_list in ticker_to_trade_info_list_dict.items():
                for trade in trade_info_list:
                    daily_pl += trade.realised_pl
                    
                    if trade.acquired_date.date() == trade.sold_date.date():
                        day_trade_summary_list.append(trade)
                    else:
                        swing_trade_summary_list.append(trade)
                
            daily_pl_dict[date] = daily_pl
        
        # Weekly Realised PL
        pl_date_list = list(daily_pl_dict.keys())
        week_realised_pl_list = []
        for date in pl_date_list:
            is_date_added = False
            
            for week_dates in week_realised_pl_list:
                if date in week_dates['week_date_list']:
                    is_date_added = True
                    break
                      
            if is_date_added:
                continue
            
            get_friday_offset_day = 4 - date.weekday()
            get_monday_offset_day = date.weekday()

            if get_monday_offset_day > 0:
                nearest_monday = date - timedelta(days=get_monday_offset_day)
            elif get_monday_offset_day == 0:
                nearest_monday = date

            if get_friday_offset_day > 0:
                nearest_friday = date + timedelta(days=get_friday_offset_day)
            elif get_friday_offset_day == 0:
                nearest_friday = date
                
            if nearest_friday.month > nearest_monday.month and date.month > nearest_monday.month:
                nearest_monday = nearest_friday.replace(day=1)
            
            if nearest_friday.month > date.month:
                last_day_of_month = (calendar.monthrange(date.year, date.month))[1]
                nearest_friday = date.replace(day=last_day_of_month)
            
            date_list = []
            for index, check_date in enumerate(pl_date_list):
                if check_date >= nearest_monday and check_date <= nearest_friday:
                    date_list.append(check_date)
                    
                    if index == len(pl_date_list) - 1:
                        week_dict = {}
                        week_dict['start_week_date'] = nearest_monday
                        week_dict['end_week_date'] = nearest_friday
                        week_dict['week_date_list'] = date_list
                        week_realised_pl_list.append(week_dict)
                        break
                else:
                    if date_list:
                        week_dict = {}
                        week_dict['start_week_date'] = nearest_monday
                        week_dict['end_week_date'] = nearest_friday
                        week_dict['week_date_list'] = date_list
                        week_realised_pl_list.append(week_dict)
                        break
        
        for week_dict in week_realised_pl_list:
            realised_pl = 0
            for date in week_dict['week_date_list']:
                realised_pl += daily_pl_dict[date]
            
            week_dict['realised_pl'] = realised_pl

        monthly_pl_dict = {}
        yearly_pl_dict = {}
        month_to_date_pl_dict = {}
        year_to_date_pl_dict = {}
        for date, pl in daily_pl_dict.items():
            year = date.year
            year_month = (year, date.month)
        
            if year_month not in monthly_pl_dict:
                monthly_pl_dict[year_month] = 0

            if year not in yearly_pl_dict:
                yearly_pl_dict[year] = 0
            
            monthly_pl_dict[year_month] += pl
            yearly_pl_dict[year] += pl
        
        for date, _ in daily_pl_dict.items():
            for check_date, pl in daily_pl_dict.items():
                if check_date.year == date.year and check_date.month == date.month and check_date.day <= date.day:
                    if date not in month_to_date_pl_dict:
                        month_to_date_pl_dict[date] = 0
                    
                    month_to_date_pl_dict[date] += pl
                elif check_date > date:
                    break
        
        for date, _ in daily_pl_dict.items():
            for check_date, pl in daily_pl_dict.items():
                if check_date.year == date.year and (check_date.month < date.month or (date.month == check_date.month and check_date.day <= date.day)):
                    if date not in year_to_date_pl_dict:
                        year_to_date_pl_dict[date] = 0
                
                    year_to_date_pl_dict[date] += pl
                elif check_date > date:
                    break
        
        # reversed_month_to_date_pl_dict = OrderedDict(reversed(sorted(month_to_date_pl_dict.items(), key=lambda t: t[0])))
        # reversed_year_to_date_pl_dict = OrderedDict(reversed(sorted(year_to_date_pl_dict.items(), key=lambda t: t[0])))
        
        # filtered_month_to_date_pl_dict = {}
        # filtered_year_to_date_pl_dict = {}
        
        # for date, pl in reversed_month_to_date_pl_dict.items():
        #     if date.year == current_date.year and date.month == current_date.month:
        #         filtered_month_to_date_pl_dict[date] = pl
        #         break
            
        # for date, pl in reversed_year_to_date_pl_dict.items():
        #     if date.year == current_date.year:
        #         filtered_year_to_date_pl_dict[date] = pl
        #         break
        
        current_date = get_current_us_datetime()
        last_us_business_day_in_end_of_year = get_last_us_business_day(current_date.year, 12)
        is_last_us_business_day_of_end_of_year = current_date.date() == last_us_business_day_in_end_of_year.date()
        is_after_last_us_business_day_weekend_or_holiday = current_date.date() > last_us_business_day_in_end_of_year.date()
        filtered_yearly_pl_dict = {}
        for year, pl in yearly_pl_dict.items():
            if (year < current_date.year
                    or is_after_last_us_business_day_weekend_or_holiday
                    or (is_last_us_business_day_of_end_of_year
                            and current_date.time() >= datetime.time(20, 0, 0))):
                filtered_yearly_pl_dict[year] = pl       
                
        filtered_monthly_pl_dict = {}
        for year_month, pl in monthly_pl_dict.items():
            year = year_month[0]
            month = year_month[1]
            
            if year < current_date.year or (year == current_date.year and month < current_date.month):
                filtered_monthly_pl_dict[year_month] = pl
            elif year == current_date.year and month == current_date.month:
                last_us_business_day_of_month = get_last_us_business_day(current_date.year, current_date.month)
                is_last_us_business_day_of_month = current_date.date() == last_us_business_day_of_month.date()
                is_after_last_us_business_day_weekend_or_holiday = current_date.date() > last_us_business_day_of_month.date()
                
                if (is_after_last_us_business_day_weekend_or_holiday
                        or (is_last_us_business_day_of_month 
                                and current_date.time() >= datetime.time(20, 0, 0))):
                    filtered_monthly_pl_dict[year_month] = pl
        
        result_dict = {}
        result_dict[DAILY_REALISED_PL] = daily_pl_dict
        result_dict[WEEKLY_REALISED_PL] = week_realised_pl_list
        result_dict[MONTH_TO_DATE_REALISED_PL] = month_to_date_pl_dict
        result_dict[YEAR_TO_DATE_REALISED_PL] = year_to_date_pl_dict
        result_dict[MONTHLY_REALISED_PL] = filtered_monthly_pl_dict
        result_dict[YEARLY_REALISED_PL] = filtered_yearly_pl_dict
        result_dict[DAY_TRADE_SUMMARY] = day_trade_summary_list
        result_dict[SWING_TRADE_SUMMARY] = swing_trade_summary_list
        
        return result_dict

    def update_realised_pl_and_trade_summary(self, realised_pl_and_trade_file_dir: str):
        transaction_df = pd.read_csv(realised_pl_and_trade_file_dir)
        normalised_transaction_df = self.__normalise_column_dtype(transaction_df)
        trade_summary_dict = self.get_trade_summary(normalised_transaction_df)
        
        daily_pl_dict = trade_summary_dict[DAILY_REALISED_PL]
        weekly_pl_dict_dict = trade_summary_dict[WEEKLY_REALISED_PL]
        month_to_date_pl_dict = trade_summary_dict[MONTH_TO_DATE_REALISED_PL]
        year_to_date_pl_dict = trade_summary_dict[YEAR_TO_DATE_REALISED_PL]
        monthly_pl_dict = trade_summary_dict[MONTHLY_REALISED_PL]
        yearly_pl_dict = trade_summary_dict[YEARLY_REALISED_PL]
        day_trade_history_list = trade_summary_dict[DAY_TRADE_SUMMARY] 
        swing_trade_history_list = trade_summary_dict[SWING_TRADE_SUMMARY]
        interest_message_list = self.__get_interest_message_dict(normalised_transaction_df)

        self.send_daily_pl_messages(daily_pl_dict, None, Broker.FIRSTRADE)
        self.send_weekly_pl_messages(weekly_pl_dict_dict, None, Broker.FIRSTRADE)
        self.send_month_to_date_pl_messages(month_to_date_pl_dict, None, Broker.FIRSTRADE)
        self.send_year_to_date_pl_messages(year_to_date_pl_dict, None, Broker.FIRSTRADE)
        self.send_monthly_pl_messages(monthly_pl_dict, None, Broker.FIRSTRADE)
        self.send_yearly_pl_messages(yearly_pl_dict, None, Broker.FIRSTRADE)
        self.send_trade_summary_message(day_trade_history_list, Broker.FIRSTRADE)
        self.send_trade_summary_message(swing_trade_history_list, Broker.FIRSTRADE)
        self.send_interest_messages(interest_message_list)
        
    def update_account_nav_value(self, account_summary_file_dir: str) -> dict:
        pdf = pdfquery.PDFQuery(account_summary_file_dir)
        pdf.load()
        account_summary_contents = pdf.pq('LTPage[pageid=\'1\'] LTTextBoxHorizontal')
        summary_dict = {}
                        
        for index, text_line in enumerate(account_summary_contents):
            if 'Total Equity Holdings' in text_line.text:
               summary_dict['opening_balance'] = float(account_summary_contents[index + 1].text.replace("$", "").replace(",", ""))
               summary_dict['closing_balance'] = float(account_summary_contents[index + 2].text.replace("$", "").replace(",", ""))
               
            date_str_match = re.search(r"([A-Za-z]+ \d{1,2}, \d{4}) - ([A-Za-z]+ \d{1,2}, \d{4})", text_line.text)
            if date_str_match:
                summary_dict['start_date'] = datetime.strptime(date_str_match.group(1), "%B %d, %Y")
                summary_dict['end_date'] = datetime.strptime(date_str_match.group(2), "%B %d, %Y")
        
        return summary_dict


        