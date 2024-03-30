import calendar
from collections import OrderedDict
from datetime import datetime, timedelta
import pandas as pd

from module.discord_chatbot_client import DiscordChatBotClient

from sql.sqlite_connector import SqliteConnector

from datasource.ib_connector import IBConnector
from datasource.pl.pl_report import PLReport

from model.pl.entry_and_exit import EntryAndExit
from model.pl.trade_profit_and_loss import TradeProfitAndLoss
from model.pl.interest_profit import InterestProfit

from constant.discord.discord_channel import DiscordChannel
from utils.datetime_util import get_current_us_datetime, get_last_us_business_day
from utils.logger import Logger

from constant.broker import Broker

logger = Logger()
idx = pd.IndexSlice

# Account Summary Fields
NAV_VALUE_FROM_DATE = 'FromDate'
NAV_VALUE_TO_DATE = 'ToDate'
NAV_STARTING_VALUE = 'StartingValue'
NAV_ENDING_VALUE = 'EndingValue'
COMMISSIONS = 'Commissions'

# Trade Fields
SYMBOL = 'Symbol'
TRADE_DATETIME = 'DateTime'
TRADE_DATE = 'TradeDate'
QUANTITY = 'Quantity'
TRADE_PRICE = 'TradePrice'
TRADE_COMMISSIONS = 'IBCommission'
AMOUNT = 'Amount'
COST = 'CostBasis'
ORDER_TYPE = 'Buy/Sell'

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

CLIENT_ACCOUNT_ID = 'ClientAccountID'
INTEREST_REPORT_DATE = 'ReportDate'
TOTAL_INTEREST = 'TotalInterest'

class IBPLReport(PLReport):
    def __init__(self, ib_connector: IBConnector, sqlite_connector: SqliteConnector, discord_client: DiscordChatBotClient) -> None:
        super().__init__(sqlite_connector, discord_client)
        self.__ib_connector = ib_connector
    
    def __normalise_account_summary_column_type(self, src_df: pd.DataFrame) -> pd.DataFrame:
        src_df[NAV_VALUE_FROM_DATE] = pd.to_datetime(src_df[NAV_VALUE_FROM_DATE], errors='coerce')
        src_df[NAV_VALUE_TO_DATE] = pd.to_datetime(src_df[NAV_VALUE_TO_DATE], errors='coerce')
        return src_df
    
    def __normalise_trade_column_type(self, src_df: pd.DataFrame) -> pd.DataFrame:
        src_df[TRADE_DATETIME] = pd.to_datetime(src_df[TRADE_DATETIME], errors='coerce')
        src_df[TRADE_DATE] = pd.to_datetime(src_df[TRADE_DATE], errors='coerce')
        src_df[TRADE_COMMISSIONS] = src_df[TRADE_COMMISSIONS].abs()
        src_df[QUANTITY] = src_df[QUANTITY].astype(int).abs()
        src_df[AMOUNT] = src_df.apply(lambda row: (row[QUANTITY] * row[TRADE_PRICE]) + row[TRADE_COMMISSIONS] if row[ORDER_TYPE] == 'BUY' else (row[QUANTITY] * row[TRADE_PRICE]) - row[TRADE_COMMISSIONS], axis=1)
        
        return src_df
    
    def __get_interest_message(self, interest_df: pd.DataFrame) -> list:
        current_date = get_current_us_datetime()
        last_us_business_day = get_last_us_business_day(current_date.year, current_date.month)
        is_on_or_after_settle_date = current_date.date() >= last_us_business_day.date()
        monthly_interest_dict = {}
        
        for row in range(len(interest_df)):
            interest_report_date = datetime.strptime(interest_df.loc[row, INTEREST_REPORT_DATE], "%Y-%m-%d")
            report_year = interest_report_date.year
            report_month = interest_report_date.month
            total_interest = float(interest_df.loc[row, TOTAL_INTEREST])
            
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
                                              paid_by=Broker.IB)
            interest_message_list.append(interest_message)

        return interest_message_list
    
    # def get_entry_and_exit_history(self, src_df: pd.DataFrame, day_trade_records: list, swing_trade_records: list) -> dict:
    #     ordered_src_df = src_df.sort_values(by=[TRADE_DATE, TRADE_DATETIME, SYMBOL]).groupby([SYMBOL, TRADE_DATE, TRADE_DATETIME, ORDER_TYPE]).agg({QUANTITY:'sum', AMOUNT: 'sum'})
    #     # ticker_to_contract_dict = self.__ib_connector.get_ticker_to_contract_dict()
        
    #     day_trade_entry_and_exit_list = []
    #     swing_trade_entry_and_exit_list = []
        
    #     for day_trade in day_trade_records:
    #         ticker = day_trade.ticker
    #         day_trade_record_date = day_trade.sold_date
            
    #         day_trade_transaction_df = ordered_src_df.loc[(ticker, day_trade_record_date)]
            
    #         accumulated_buy_share = 0
    #         accumulated_buy_cost = 0

    #         day_trade_buy_datetime_list = []
    #         day_trade_buy_quantity_list = []
    #         day_trade_entry_price_list = []
    #         day_trade_sell_datetime_list = []
    #         day_trade_sell_quantity_list = []
    #         day_trade_exit_price_list = []
            
    #         day_trade_realised_pl_list = []
    #         day_trade_realised_pl_percent_list = [] 
            
    #         for index in day_trade_transaction_df.index:
    #             trade_datetime = index[0]
    #             second_truncated_trade_datetime = trade_datetime.replace(second=0, microsecond=0)
    #             order_type = index[1]
                
    #             amount = day_trade_transaction_df.loc[index, AMOUNT]
    #             quantity = day_trade_transaction_df.loc[index, QUANTITY]
                
    #             if order_type == 'BUY':
    #                 entry_price = amount / quantity
    #                 accumulated_buy_share += quantity
    #                 accumulated_buy_cost += amount
                    
    #                 day_trade_buy_datetime_list.append(second_truncated_trade_datetime)
    #                 day_trade_buy_quantity_list.append(quantity)
    #                 day_trade_entry_price_list.append(entry_price)
    #             else: 
    #                 exit_price = amount / quantity

    #                 if accumulated_buy_share == quantity:
    #                     realised_pl = amount - accumulated_buy_cost
    #                     realised_pl_percent = round(((realised_pl / accumulated_buy_cost) * 100), 3)
    #                     accumulated_buy_cost = 0
    #                     accumulated_buy_share = 0
    #                 else:
    #                     adjusted_cost = (accumulated_buy_cost / accumulated_buy_share) * quantity
    #                     realised_pl = amount - adjusted_cost
    #                     realised_pl_percent = round(((realised_pl / adjusted_cost) * 100), 3)
    #                     accumulated_buy_cost = accumulated_buy_cost - adjusted_cost
    #                     accumulated_buy_share = accumulated_buy_share - quantity
                        
    #                 day_trade_sell_datetime_list.append(second_truncated_trade_datetime)
    #                 day_trade_sell_quantity_list.append(quantity)
    #                 day_trade_exit_price_list.append(exit_price)
                    
    #                 day_trade_realised_pl_list.append(realised_pl)
    #                 day_trade_realised_pl_percent_list.append(realised_pl_percent)
            
    #         entry_and_exit = EntryAndExit(ticker=ticker,
    #                                       realised_pl_list=day_trade_realised_pl_list, realised_pl_percent_list=day_trade_realised_pl_percent_list,
    #                                       buy_quantity_list=day_trade_buy_quantity_list, sell_quantity_list=day_trade_sell_quantity_list,
    #                                       buy_datetime_list=day_trade_buy_datetime_list, sell_datetime_list=day_trade_sell_datetime_list,
    #                                       contract_info={},
    #                                       trading_platform=Broker.IB)
    #         day_trade_entry_and_exit_list.append(entry_and_exit)
    #         # candle_df = self.__ib_connector.get_historical_candle_df([ticker_to_contract_dict[ticker]], '960min' , bar_size, outside_rth_str, candle_retrieval_start_dt)

    #     for swing_trade in swing_trade_records:
    #         ticker = day_trade.ticker
    #         acquire_date = swing_trade.acquired_date
    #         sold_date = swing_trade.sold_date
        
    #         swing_trade_transaction_df = ordered_src_df.loc[idx[ticker, acquire_date:sold_date, order_type], :]
    #         ticker_trade_date_list = [index[1] for index in swing_trade_transaction_df.index]
            
    #         swing_trade_buy_datetime_list = []
    #         swing_trade_buy_quantity_list = []
    #         swing_trade_entry_price_list = []
    #         swing_trade_sell_datetime_list = []
    #         swing_trade_sell_quantity_list = []
    #         swing_trade_exit_price_list = []
            
    #         swing_trade_realised_pl_list = []
    #         swing_trade_realised_pl_percent_list = [] 
            
    #         total_previous_day_remaining_cost = 0
    #         total_previous_day_remaining_share = 0
            
    #         for ticker_trade_date in ticker_trade_date_list:
    #             intra_day_transaction_df = ordered_src_df.loc[(ticker, ticker_trade_date)]
                
    #             total_intra_day_remaining_share = 0
    #             total_intra_day_remaining_cost = 0
                
    #             for index in intra_day_transaction_df.index:
    #                 order_type = index[1]
                    
    #                 amount = intra_day_transaction_df.loc[index, AMOUNT]
    #                 quantity = intra_day_transaction_df.loc[index, QUANTITY]
                    
    #                 if order_type == 'BUY':
    #                     total_intra_day_remaining_share += quantity
    #                     total_intra_day_remaining_cost += amount
    #                 else: 
    #                     avg_entry_price = total_intra_day_remaining_cost / total_intra_day_remaining_share
    #                     adjusted_cost = avg_entry_price * quantity if total_intra_day_remaining_share != quantity else total_intra_day_remaining_cost
                        
    #                     # if total_previous_day_remaining_share > 0:
    #                     #     total_intra_day_remaining_share = 
                                 
    #                     total_intra_day_remaining_share = total_intra_day_remaining_share - quantity
    #                     total_intra_day_remaining_cost = total_intra_day_remaining_cost - adjusted_cost
                        
    #             if total_intra_day_remaining_share > 0:
    #                 #should consider avg price as entry price for swing?
    #                 total_previous_day_remaining_cost = total_intra_day_remaining_cost
    #                 total_previous_day_reamining_share = total_intra_day_remaining_share
    #                 swing_trade_buy_datetime_list.append(ticker_trade_date)
    #                 swing_trade_buy_quantity_list.append(total_intra_day_remaining_share)
    #                 swing_trade_entry_price_list.append(total_intra_day_remaining_cost / total_intra_day_remaining_share)
                    
    #             # accumulated_buy_share = 0
    #             # accumulated_buy_cost = 0

    #             # buy_datetime_list = []
    #             # buy_quantity_list = []
    #             # entry_price_list = []
    #             # sell_datetime_list = []
    #             # sell_quantity_list = []
    #             # exit_price_list = []

    #             # realised_pl_list = []
    #             # realised_pl_percent_list = [] 
                
    def get_trade_summary(self, src_df: pd.DataFrame):
        buy_order_boolean_df = (src_df[ORDER_TYPE] == 'BUY')
        sell_order_boolean_df = (src_df[ORDER_TYPE] == 'SELL')
        buy_order_df = src_df[buy_order_boolean_df].groupby([SYMBOL, TRADE_DATE]).agg({QUANTITY:'sum', AMOUNT: 'sum', TRADE_COMMISSIONS: 'sum'})
        sell_order_df = src_df[sell_order_boolean_df].groupby([SYMBOL, TRADE_DATE]).agg({QUANTITY:'sum', AMOUNT: 'sum', TRADE_COMMISSIONS: 'sum'})
        
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

        ordered_src_df = src_df.sort_values(by=[SYMBOL, TRADE_DATE, ORDER_TYPE]).groupby([SYMBOL, TRADE_DATE, ORDER_TYPE]).agg({QUANTITY:'sum', AMOUNT: 'sum'})
        
        date_to_ticker_transaction_dict = {}
        for ticker, trade_history_datetime_list in ticker_to_trade_history_datetime_list_dict.items():
            accumulated_buy_cost = 0
            accumulated_buy_share = 0
            
            previous_day_remaining_cost = 0
            previous_day_remaining_position = 0
            
            earliest_fillied_buy_order_datetime = None
            
            for date in trade_history_datetime_list:
                pl_message_list = []
                
                buy_order_exist = (ticker, date, 'BUY') in ordered_src_df.index
                sell_order_exist = (ticker, date, 'SELL') in ordered_src_df.index
                
                if buy_order_exist:
                    buy_cost = ordered_src_df.loc[(ticker, date, 'BUY'), AMOUNT]
                    buy_quantity = ordered_src_df.loc[(ticker, date, 'BUY'), QUANTITY]
                    avg_entry_price = buy_cost / buy_quantity
                    buy_date = date
                    
                    accumulated_buy_share += buy_quantity
                    accumulated_buy_cost += buy_cost
                        
                    if not earliest_fillied_buy_order_datetime:
                        earliest_fillied_buy_order_datetime = buy_date
                    
                if sell_order_exist:
                    if accumulated_buy_share <= 0:
                        logger.log_debug_msg(f'Exclude invalid data for {ticker} on {date}, {ticker} full date list: {trade_history_datetime_list}', with_std_out=True)
                        continue
                    
                    sell_market_value = ordered_src_df.loc[(ticker, date, 'SELL'), AMOUNT]
                    sell_quantity = ordered_src_df.loc[(ticker, date, 'SELL'), QUANTITY]
                    avg_exit_price = sell_market_value / sell_quantity
                    sell_date = date
                    
                    if previous_day_remaining_position > 0 and accumulated_buy_share > previous_day_remaining_position:
                        avg_entry_price_for_previous_day_position = previous_day_remaining_cost / previous_day_remaining_position
                        adjusted_cost_for_previous_day_position = avg_entry_price_for_previous_day_position * previous_day_remaining_position
                        adjusted_sell_market_value = avg_exit_price * previous_day_remaining_position
                        realised_pl_for_previous_day_position = adjusted_sell_market_value - adjusted_cost_for_previous_day_position
                        realised_pl_percent_for_previous_day_position = round(((realised_pl_for_previous_day_position / adjusted_cost_for_previous_day_position) * 100), 3)
                        
                        if abs(round(realised_pl_for_previous_day_position)) > 0:
                            pl_message_param_dict = dict(ticker=ticker, 
                                                         acquired_date=earliest_fillied_buy_order_datetime, sold_date=sell_date,
                                                         realised_pl=round(realised_pl_for_previous_day_position), realised_pl_percent=realised_pl_percent_for_previous_day_position,
                                                         accumulated_cost=accumulated_buy_cost, adjusted_cost=adjusted_cost_for_previous_day_position, market_value=round(adjusted_sell_market_value),
                                                         accumulated_shares=accumulated_buy_share, sell_quantity=previous_day_remaining_position, remaining_positions=(accumulated_buy_share - previous_day_remaining_position),
                                                         avg_entry_price=round(avg_entry_price_for_previous_day_position, 3), avg_exit_price=round(avg_exit_price, 3),
                                                         trading_platform=Broker.IB,
                                                         contract_info={})
                            pl_message = TradeProfitAndLoss(**pl_message_param_dict)
                            pl_message_list.append(pl_message)
                     
                        accumulated_buy_cost = accumulated_buy_cost - adjusted_cost_for_previous_day_position
                        accumulated_buy_share = accumulated_buy_share - previous_day_remaining_position
                        sell_quantity = sell_quantity - previous_day_remaining_position
                        sell_market_value = sell_market_value - adjusted_sell_market_value
                        earliest_fillied_buy_order_datetime = date
                        
                    avg_entry_price = accumulated_buy_cost / accumulated_buy_share
                    avg_exit_price = sell_market_value / sell_quantity
                    adjusted_cost = avg_entry_price * sell_quantity
                    realised_pl = sell_market_value - adjusted_cost
                    realised_pl_percent = round(((realised_pl / adjusted_cost) * 100), 3)
                    
                    if abs(round(realised_pl)) > 0:
                        pl_message_param_dict = dict(ticker=ticker, 
                                                     acquired_date=earliest_fillied_buy_order_datetime, sold_date=sell_date,
                                                     realised_pl=round(realised_pl), realised_pl_percent=realised_pl_percent,
                                                     accumulated_cost=accumulated_buy_cost, adjusted_cost=adjusted_cost, market_value=round(sell_market_value),
                                                     accumulated_shares=accumulated_buy_share, sell_quantity=sell_quantity, remaining_positions=(accumulated_buy_share - sell_quantity),
                                                     avg_entry_price=round(avg_entry_price, 3), avg_exit_price=round(avg_exit_price, 3),
                                                     trading_platform=Broker.IB,
                                                     contract_info={})
                        pl_message = TradeProfitAndLoss(**pl_message_param_dict)
                        pl_message_list.append(pl_message)
                    
                    previous_day_remaining_position = accumulated_buy_share - sell_quantity
                    previous_day_remaining_cost = accumulated_buy_cost - adjusted_cost
                    
                    accumulated_buy_share = accumulated_buy_share - sell_quantity
                    accumulated_buy_cost = accumulated_buy_cost - adjusted_cost
                    
                    if previous_day_remaining_position == 0:
                        previous_day_remaining_cost = 0
                    
                    if accumulated_buy_share == 0:
                        accumulated_buy_cost = 0
                        earliest_fillied_buy_order_datetime = None
                
                if pl_message_list:
                    if date not in date_to_ticker_transaction_dict:
                        date_to_ticker_transaction_dict[date] = {}
                    
                    date_to_ticker_transaction_dict[date][ticker] = pl_message_list
                else:
                    logger.log_debug_msg(f'No sell order for {ticker} on {date} found in IB', with_std_out=True)
                  
        close_position_date_to_trade_summary_list_dict = OrderedDict(sorted(date_to_ticker_transaction_dict.items(), key=lambda t: t[0]))
        
        # Daily Realised PL, Day Trade Summary, Swing Trade Summary
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

        # Yearly and Monthly Realised PL
        monthly_pl_dict = {}
        yearly_pl_dict = {}
        for date, pl in daily_pl_dict.items():
            year = date.year
            year_month = (year, date.month)
        
            if year_month not in monthly_pl_dict:
                monthly_pl_dict[year_month] = 0

            if year not in yearly_pl_dict:
                yearly_pl_dict[year] = 0
            
            monthly_pl_dict[year_month] += pl
            yearly_pl_dict[year] += pl
        
        # Month to Date Realised PL
        month_to_date_pl_dict = {}
        for date, _ in daily_pl_dict.items():
            for check_date, pl in daily_pl_dict.items():
                if check_date.year == date.year and check_date.month == date.month and check_date.day <= date.day:
                    if date not in month_to_date_pl_dict:
                        month_to_date_pl_dict[date] = 0
                    
                    month_to_date_pl_dict[date] += pl
                elif check_date > date:
                    break
        
        # Year to Date Realised PL
        year_to_date_pl_dict = {}
        for date, _ in daily_pl_dict.items():
            for check_date, pl in daily_pl_dict.items():
                if check_date.year == date.year and (check_date.month < date.month or (date.month == check_date.month and check_date.day <= date.day)):
                    if date not in year_to_date_pl_dict:
                        year_to_date_pl_dict[date] = 0
                
                    year_to_date_pl_dict[date] += pl
                elif check_date > date:
                    break
        
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
    
    def update_realised_pl_and_trade_summary(self, trade_data_file_dir: str):
        specific_rows = [0, 1]
        account_summary_df = pd.read_csv(trade_data_file_dir, on_bad_lines='skip', skiprows = lambda x: x not in specific_rows)
        transaction_df  = pd.read_csv(trade_data_file_dir, on_bad_lines='skip', skiprows = lambda x: x in specific_rows)
        
        interest_header_boolean_df = (transaction_df[CLIENT_ACCOUNT_ID] == 'InterestType')
        interest_value_boolean_df = (transaction_df[CLIENT_ACCOUNT_ID] == 'Debit Interest') | (transaction_df[CLIENT_ACCOUNT_ID] == 'Credit Interest')
        
        interest_header_df = transaction_df[interest_header_boolean_df]
        interest_data_df = transaction_df[interest_value_boolean_df]
        
        interest_header_value = interest_header_df.values[0]        
        interest_df = pd.DataFrame(columns=interest_header_value, data=interest_data_df.values).dropna(axis=1, how='all')
        trade_df = transaction_df[~(interest_header_boolean_df | interest_value_boolean_df)]
        trade_df = trade_df[trade_df[SYMBOL].str.contains('^[A-Z]{0,4}$', regex=True, na=False)]
        
        normalised_account_summary_df = self.__normalise_account_summary_column_type(account_summary_df)
        normalised_trade_df = self.__normalise_trade_column_type(trade_df)
        trade_summary_dict = self.get_trade_summary(trade_df)
        
        daily_pl_dict = trade_summary_dict[DAILY_REALISED_PL] 
        weekly_pl_dict_dict = trade_summary_dict[WEEKLY_REALISED_PL]
        month_to_date_pl_dict = trade_summary_dict[MONTH_TO_DATE_REALISED_PL] 
        year_to_date_pl_dict = trade_summary_dict[YEAR_TO_DATE_REALISED_PL] 
        monthly_pl_dict = trade_summary_dict[MONTHLY_REALISED_PL] 
        yearly_pl_dict = trade_summary_dict[YEARLY_REALISED_PL] 
        day_trade_history_list = trade_summary_dict[DAY_TRADE_SUMMARY] 
        swing_trade_history_list = trade_summary_dict[SWING_TRADE_SUMMARY]
        interest_message_list = self.__get_interest_message(interest_df)
        
        #entry_and_exit_dict = self.get_entry_and_exit_history(trade_df, day_trade_history_list, swing_trade_history_list)
                
        self.send_daily_pl_messages(daily_pl_dict, None, Broker.IB)
        self.send_weekly_pl_messages(weekly_pl_dict_dict, None, Broker.IB)
        self.send_month_to_date_pl_messages(month_to_date_pl_dict, None, Broker.IB)
        self.send_year_to_date_pl_messages(year_to_date_pl_dict, None, Broker.IB)
        self.send_monthly_pl_messages(monthly_pl_dict, None, Broker.IB)
        self.send_yearly_pl_messages(yearly_pl_dict, None, Broker.IB)
        self.send_trade_summary_message(day_trade_history_list, Broker.IB, DiscordChannel.IB_DAY_TRADE_SUMMARY)
        self.send_trade_summary_message(swing_trade_history_list, Broker.IB, DiscordChannel.IB_SWING_TRADE_SUMMARY)
        self.send_interest_messages(interest_message_list, Broker.IB)
