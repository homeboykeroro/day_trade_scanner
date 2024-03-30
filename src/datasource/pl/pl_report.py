from abc import ABC
import calendar
from datetime import datetime

from module.discord_chatbot_client import DiscordChatBotClient

from sql.sqlite_connector import SqliteConnector

from model.pl.daily_profit_and_loss import DailyProfitAndLoss
from model.pl.weekly_profit_and_loss import WeeklyProfitAndLoss
from model.pl.month_to_date_profit_and_loss import MonthToDateProfitAndLoss
from model.pl.year_to_date_profit_and_loss import YearToDateProfitAndLoss
from model.pl.monthly_profit_and_loss import MonthlyProfitAndLoss
from model.pl.yearly_profit_and_loss import YearlyProfitAndLoss

from utils.discord_message_record_util import add_sent_aggregated_daily_pl_records, add_sent_aggregated_month_to_date_pl_records, add_sent_aggregated_monthly_records, add_sent_aggregated_weekly_pl_records, add_sent_aggregated_year_to_date_pl_records, add_sent_aggregated_yearly_pl_records, add_sent_daily_realised_pl_message_record, add_sent_interest_history_message_record, add_sent_month_to_date_realised_pl_message_record, add_sent_monthly_realised_pl_message_record, add_sent_trade_summary_message_record, add_sent_weekly_realised_pl_message_record, add_sent_year_to_date_realised_pl_message_record, add_sent_yearly_realised_pl_message_record, check_if_daily_realised_pl_message_sent, check_if_interest_history_message_sent, check_if_month_to_date_realised_pl_message_sent, check_if_monthly_realised_pl_message_sent, check_if_trade_summary_message_sent, check_if_weekly_realised_pl_message_sent, check_if_year_to_date_realised_pl_message_sent, check_if_yearly_realised_pl_message_sent, delete_all_sent_daily_realised_pl_message_record, delete_all_sent_imported_pl_file_message_record, delete_all_sent_interest_history_message_record, delete_all_sent_month_to_date_realised_pl_message_record, delete_all_sent_monthly_realised_pl_message_record, delete_all_sent_trade_summary_message_record, delete_all_sent_weekly_realised_pl_message_record, delete_all_sent_year_to_date_realised_pl_message_record, delete_all_sent_yearly_realised_pl_message_record, get_all_aggregated_daily_pl_records, get_all_aggregated_month_to_date_pl_records, get_all_aggregated_monthly_pl_records, get_all_aggregated_weekly_pl_records, get_all_aggregated_year_to_date_pl_records, get_all_aggregated_yearly_pl_records

from constant.broker import Broker
from constant.discord.discord_channel import DiscordChannel

class PLReport(ABC):
    def __init__(self, sqlite_connector: SqliteConnector, discord_client: DiscordChatBotClient) -> None:
        self.__sqlite_connector = sqlite_connector
        self.__discord_client = discord_client
    
    def __get_daily_realised_pl_message(self, daily_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker) -> list:
        send_daily_realised_pl_message_list = []
        for date, daily_realised_pl in daily_realised_pl_dict.items():
            if_message_sent = check_if_daily_realised_pl_message_sent(connector=self.__sqlite_connector, 
                                                                      settle_date=date, 
                                                                      trading_platform=trading_platform)
            if not if_message_sent:
                daily_realised_pl_message = DailyProfitAndLoss(settle_date=date, 
                                                               realised_pl=daily_realised_pl,
                                                               account_value=account_nav_value,
                                                               trading_platform=trading_platform)
                send_daily_realised_pl_message_list.append(daily_realised_pl_message)
        
        return send_daily_realised_pl_message_list
                
    def __get_weekly_realised_pl_message(self, weekly_realised_pl_dict_list: dict, account_nav_value: float, trading_platform: Broker) -> list:
        send_weekly_realised_pl_message_list = []
        for weekly_realised_pl_dict in weekly_realised_pl_dict_list:
            if_message_sent = check_if_weekly_realised_pl_message_sent(connector=self.__sqlite_connector,
                                                                       start_week_date=weekly_realised_pl_dict['start_week_date'],
                                                                       end_week_date=weekly_realised_pl_dict['end_week_date'],
                                                                       trading_platform=trading_platform)
            if not if_message_sent:
                weekly_realised_pl_message = WeeklyProfitAndLoss(start_week_date=weekly_realised_pl_dict['start_week_date'],
                                                                end_week_date=weekly_realised_pl_dict['end_week_date'],
                                                                realised_pl=weekly_realised_pl_dict['realised_pl'],
                                                                account_value=account_nav_value,
                                                                trading_platform=trading_platform)
                send_weekly_realised_pl_message_list.append(weekly_realised_pl_message)
                
        return send_weekly_realised_pl_message_list
    
    def __get_month_to_date_realised_pl_message(self, month_to_date_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker) -> list:
        send_month_to_date_realised_pl_message_list = []
        for date, month_to_date_realised_pl in month_to_date_realised_pl_dict.items():
            if_message_sent = check_if_month_to_date_realised_pl_message_sent(connector=self.__sqlite_connector,
                                                                              settle_date=date,
                                                                              trading_platform=trading_platform)
            if not if_message_sent:
                month_to_date_realised_pl_message = MonthToDateProfitAndLoss(settle_date=date, 
                                                                             realised_pl=month_to_date_realised_pl,
                                                                             account_value=account_nav_value,
                                                                             trading_platform=trading_platform)
                send_month_to_date_realised_pl_message_list.append(month_to_date_realised_pl_message)
        
        return send_month_to_date_realised_pl_message_list
    
    def __get_year_to_date_realised_pl_message(self, year_to_date_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker) -> list:
        send_year_to_date_realised_pl_message_list = []
        for date, year_to_date_realised_pl in year_to_date_realised_pl_dict.items():
            if_message_sent = check_if_year_to_date_realised_pl_message_sent(connector=self.__sqlite_connector,
                                                                              settle_date=date,
                                                                              trading_platform=trading_platform)
            if not if_message_sent:
                year_to_date_realised_pl_message = YearToDateProfitAndLoss(settle_date=date, 
                                                                           realised_pl=year_to_date_realised_pl,
                                                                           account_value=account_nav_value,
                                                                           trading_platform=trading_platform)
                send_year_to_date_realised_pl_message_list.append(year_to_date_realised_pl_message)
        
        return send_year_to_date_realised_pl_message_list
    
    def __get_monthly_realised_pl_message(self, monthly_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker) -> list:
        send_monthly_realised_pl_message_list = []
        for year_month, monthly_realised_pl in monthly_realised_pl_dict.items():
            calendar_range = calendar.monthrange(year_month[0], year_month[1])
            start_month_date = datetime(day=1, year=year_month[0], month=year_month[1])
            end_month_date = datetime(day=calendar_range[1], year=year_month[0], month=year_month[1])
        
            if_message_sent = check_if_monthly_realised_pl_message_sent(connector=self.__sqlite_connector,
                                                                        start_month_date=start_month_date, 
                                                                        end_month_date=end_month_date,
                                                                        trading_platform=trading_platform)
            if not if_message_sent:
                monthly_realised_pl_message = MonthlyProfitAndLoss(start_month_date=start_month_date, 
                                                                   end_month_date=end_month_date,
                                                                   realised_pl=monthly_realised_pl,
                                                                   account_value=account_nav_value,
                                                                   trading_platform=trading_platform)
                send_monthly_realised_pl_message_list.append(monthly_realised_pl_message)
        
        return send_monthly_realised_pl_message_list    

    def __get_yearly_realised_pl_message(self, yearly_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker) -> list:
        send_yearly_realised_pl_message_list = []
        for year, yearly_realised_pl in yearly_realised_pl_dict.items():
            calendar_range = calendar.monthrange(year, 12)
            start_year_date = datetime(day=1, year=year, month=1)
            end_year_date = datetime(day=calendar_range[1], year=year, month=12)
        
            if_message_sent = check_if_yearly_realised_pl_message_sent(connector=self.__sqlite_connector,
                                                                        start_year_date=start_year_date, 
                                                                        end_year_date=end_year_date,
                                                                        trading_platform=trading_platform)
            if not if_message_sent:
                yearly_realised_pl_message = YearlyProfitAndLoss(start_year_date=start_year_date, 
                                                                 end_year_date=end_year_date,
                                                                 realised_pl=yearly_realised_pl,
                                                                 account_value=account_nav_value,
                                                                 trading_platform=trading_platform)
                send_yearly_realised_pl_message_list.append(yearly_realised_pl_message)
        
        return send_yearly_realised_pl_message_list
    
    def __get_trade_summary_message(self, trade_summary_list: dict, trading_platform: Broker) -> list:
        send_trade_summary_message_list = []
        for trade_summary in trade_summary_list:
            if_message_sent = check_if_trade_summary_message_sent(connector=self.__sqlite_connector,
                                                                  ticker=trade_summary.ticker,
                                                                  acquired_date=trade_summary.acquired_date, 
                                                                  sold_date=trade_summary.sold_date,
                                                                  trading_platform=trading_platform)
            if not if_message_sent:
                send_trade_summary_message_list.append(trade_summary)
        
        return send_trade_summary_message_list
    
    def send_daily_pl_messages(self, daily_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker):
        daily_realised_pl_message_list = self.__get_daily_realised_pl_message(daily_realised_pl_dict, account_nav_value, trading_platform)
        
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_DAILY_PROFIT_AND_LOSS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_DAILY_PROFIT_AND_LOSS
        
        for daily_realised_pl_message in daily_realised_pl_message_list:
            self.__discord_client.send_message_by_list_with_response([daily_realised_pl_message], channel)
            add_sent_daily_realised_pl_message_record(connector=self.__sqlite_connector,
                                                      message_list=[daily_realised_pl_message])
            
    def send_weekly_pl_messages(self, weekly_realised_pl_dict_list: dict, account_nav_value: float, trading_platform: Broker):
        weekly_realised_pl_message_list = self.__get_weekly_realised_pl_message(weekly_realised_pl_dict_list, account_nav_value, trading_platform)
        
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_WEEKY_PROFIT_AND_LOSS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_WEEKY_PROFIT_AND_LOSS
        
        for weekly_realised_pl_message in weekly_realised_pl_message_list:
            self.__discord_client.send_message_by_list_with_response([weekly_realised_pl_message], channel)
            add_sent_weekly_realised_pl_message_record(connector=self.__sqlite_connector,
                                                              message_list=[weekly_realised_pl_message])
    
    def send_month_to_date_pl_messages(self, month_to_date_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker):
        month_to_date_realised_pl_message_list = self.__get_month_to_date_realised_pl_message(month_to_date_realised_pl_dict, account_nav_value, trading_platform)
        
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_MONTH_TO_DATE_PROFIT_AND_LOSS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_MONTH_TO_DATE_PROFIT_AND_LOSS
        
        for month_to_date_realised_pl_message in month_to_date_realised_pl_message_list:
            self.__discord_client.send_message_by_list_with_response([month_to_date_realised_pl_message], channel)
            add_sent_month_to_date_realised_pl_message_record(connector=self.__sqlite_connector,
                                                              message_list=[month_to_date_realised_pl_message])
    
    def send_year_to_date_pl_messages(self, year_to_date_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker):
        year_to_date_realised_pl_message_list = self.__get_year_to_date_realised_pl_message(year_to_date_realised_pl_dict, account_nav_value, trading_platform)
        
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_YEAR_TO_DATE_PROFIT_AND_LOSS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_YEAR_TO_DATE_PROFIT_AND_LOSS
        
        for year_to_date_realised_pl_message in year_to_date_realised_pl_message_list:
            self.__discord_client.send_message_by_list_with_response([year_to_date_realised_pl_message], channel)
            add_sent_year_to_date_realised_pl_message_record(connector=self.__sqlite_connector,
                                                              message_list=[year_to_date_realised_pl_message])
    
    def send_monthly_pl_messages(self, monthly_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker):  
        monthly_realised_pl_message_list = self.__get_monthly_realised_pl_message(monthly_realised_pl_dict, account_nav_value, trading_platform)
        
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_MONTHLY_PROFIT_AND_LOSS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_MONTHLY_PROFIT_AND_LOSS
        
        for monthly_realised_pl_message in monthly_realised_pl_message_list:
            self.__discord_client.send_message_by_list_with_response([monthly_realised_pl_message], channel)
            add_sent_monthly_realised_pl_message_record(connector=self.__sqlite_connector,
                                                              message_list=[monthly_realised_pl_message]) 
            
    def send_yearly_pl_messages(self, yearly_realised_pl_dict: dict, account_nav_value: float, trading_platform: Broker):
        yearly_realised_pl_message_list = self.__get_yearly_realised_pl_message(yearly_realised_pl_dict, account_nav_value, trading_platform)
        
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_YEARLY_PROFIT_AND_LOSS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_YEARLY_PROFIT_AND_LOSS
        
        for yearly_realised_pl_message in yearly_realised_pl_message_list:
            self.__discord_client.send_message_by_list_with_response([yearly_realised_pl_message], channel)
            add_sent_yearly_realised_pl_message_record(connector=self.__sqlite_connector,
                                                              message_list=[yearly_realised_pl_message]) 

    def send_interest_messages(self, interest_list: list, trading_platform: Broker):
        channel = None
        if trading_platform == Broker.FIRSTRADE:
            channel = DiscordChannel.FIRSTRADE_INTERESTS
        elif trading_platform == Broker.IB:
            channel = DiscordChannel.IB_INTERESTS
        
        for interest_message in interest_list:
            self.__discord_client.send_message_by_list_with_response([interest_message], channel)
            add_sent_interest_history_message_record(connector=self.__sqlite_connector,
                                                     message_list=[interest_message]) 
    
    def send_trade_summary_message(self, day_trade_summary_list: list, channel: DiscordChannel):
        trade_summary_message_list = self.__get_trade_summary_message(day_trade_summary_list, channel)
        for trade_summary_message in trade_summary_message_list:
            self.__discord_client.send_message_by_list_with_response([trade_summary_message], DiscordChannel.DEVELOPMENT_TEST)
            add_sent_trade_summary_message_record(connector=self.__sqlite_connector,
                                                  message_list=[trade_summary_message]) 
    
    @staticmethod
    def clear_all_pl_and_trade_records(sqlite_connector: SqliteConnector):
        delete_all_sent_trade_summary_message_record(sqlite_connector)
        delete_all_sent_daily_realised_pl_message_record(sqlite_connector)
        delete_all_sent_weekly_realised_pl_message_record(sqlite_connector)
        delete_all_sent_monthly_realised_pl_message_record(sqlite_connector)
        delete_all_sent_yearly_realised_pl_message_record(sqlite_connector)
        delete_all_sent_month_to_date_realised_pl_message_record(sqlite_connector)
        delete_all_sent_year_to_date_realised_pl_message_record(sqlite_connector)
        delete_all_sent_interest_history_message_record(sqlite_connector)
        delete_all_sent_imported_pl_file_message_record(sqlite_connector)
    
    @staticmethod
    def send_aggregated_messages(sqlite_connector: SqliteConnector, discord_client: DiscordChatBotClient):
        aggregated_daily_pl_record_list = get_all_aggregated_daily_pl_records(sqlite_connector)
        aggregated_daily_pl_message_list = []
        for aggregated_daily_pl_record in aggregated_daily_pl_record_list:
            daily_pl_message = DailyProfitAndLoss(settle_date=datetime.strptime(aggregated_daily_pl_record[0], "%Y-%m-%d %H:%M:%S"),
                                                  realised_pl=aggregated_daily_pl_record[1])
            aggregated_daily_pl_message_list.append(daily_pl_message)
        
        for aggregated_daily_pl_message in aggregated_daily_pl_message_list:
            discord_client.send_message_by_list_with_response([aggregated_daily_pl_message], DiscordChannel.DAILY_PROFIT_AND_LOSS)
            add_sent_aggregated_daily_pl_records(connector=sqlite_connector,
                                                  message_list=[aggregated_daily_pl_message]) 
        
        aggregated_weekly_pl_record_list = get_all_aggregated_weekly_pl_records(sqlite_connector)
        aggregated_weekly_pl_message_list = []
        for aggregated_weekly_pl_record in aggregated_weekly_pl_record_list:
            weekly_pl_message = WeeklyProfitAndLoss(start_week_date=datetime.strptime(aggregated_weekly_pl_record[0], "%Y-%m-%d %H:%M:%S"),
                                                    end_week_date=datetime.strptime(aggregated_weekly_pl_record[1], "%Y-%m-%d %H:%M:%S"),
                                                    realised_pl=aggregated_weekly_pl_record[2])
            aggregated_weekly_pl_message_list.append(weekly_pl_message)
        
        for weekly_realised_pl_message in aggregated_weekly_pl_message_list:
            discord_client.send_message_by_list_with_response([weekly_realised_pl_message], DiscordChannel.WEEKLY_PROFIT_AND_LOSS)
            add_sent_aggregated_weekly_pl_records(connector=sqlite_connector,
                                                  message_list=[weekly_realised_pl_message])
        
        aggregated_month_to_date_pl_record_list = get_all_aggregated_month_to_date_pl_records(sqlite_connector)
        aggregated_month_to_date_pl_message_list = []
        for aggregated_month_to_date_pl_record in aggregated_month_to_date_pl_record_list:
            month_to_date_pl_message = MonthToDateProfitAndLoss(settle_date=datetime.strptime(aggregated_month_to_date_pl_record[0], "%Y-%m-%d %H:%M:%S"),
                                                                realised_pl=aggregated_month_to_date_pl_record[1])
            aggregated_month_to_date_pl_message_list.append(month_to_date_pl_message)
            
        for aggregated_month_to_date_pl_message in aggregated_month_to_date_pl_message_list:
            discord_client.send_message_by_list_with_response([aggregated_month_to_date_pl_message], DiscordChannel.MONTH_TO_DATE_PROFIT_AND_LOSS)
            add_sent_aggregated_month_to_date_pl_records(connector=sqlite_connector,
                                                         message_list=[aggregated_month_to_date_pl_message])
        
        aggregated_year_to_date_pl_record_list = get_all_aggregated_year_to_date_pl_records(sqlite_connector)
        aggregated_year_to_date_pl_message_list = []
        for aggregated_year_to_date_pl_record in aggregated_year_to_date_pl_record_list:
            year_to_date_pl_message = YearToDateProfitAndLoss(settle_date=datetime.strptime(aggregated_year_to_date_pl_record[0], "%Y-%m-%d %H:%M:%S"),
                                                              realised_pl=aggregated_year_to_date_pl_record[1])
            aggregated_year_to_date_pl_message_list.append(year_to_date_pl_message)
        
        for aggregated_year_to_date_pl_message in aggregated_year_to_date_pl_message_list:
            discord_client.send_message_by_list_with_response([aggregated_year_to_date_pl_message], DiscordChannel.YEAR_TO_DATE_PROFIT_AND_LOSS)
            add_sent_aggregated_year_to_date_pl_records(connector=sqlite_connector,
                                                        message_list=[aggregated_year_to_date_pl_message])
        
        aggregated_monthly_pl_record_list = get_all_aggregated_monthly_pl_records(sqlite_connector)
        aggregated_monthly_pl_message_list = []
        for aggregated_monthly_pl_record in aggregated_monthly_pl_record_list:
            monthly_pl_message = MonthlyProfitAndLoss(start_month_date=datetime.strptime(aggregated_monthly_pl_record[0], "%Y-%m-%d %H:%M:%S"),
                                                      end_month_date=datetime.strptime(aggregated_monthly_pl_record[1], "%Y-%m-%d %H:%M:%S"),
                                                      realised_pl=aggregated_monthly_pl_record[2])
            aggregated_monthly_pl_message_list.append(monthly_pl_message)
            
        for aggregated_monthly_pl_message in aggregated_monthly_pl_message_list:
            discord_client.send_message_by_list_with_response([aggregated_monthly_pl_message], DiscordChannel.MONTHLY_PROFIT_AND_LOSS)
            add_sent_aggregated_monthly_records(connector=sqlite_connector,
                                                message_list=[aggregated_monthly_pl_message])
        
        aggregated_yearly_pl_record_list = get_all_aggregated_yearly_pl_records(sqlite_connector)
        aggregated_yearly_pl_message_list = []   
        for aggregated_yearly_pl_record in aggregated_yearly_pl_record_list:
            yearly_pl_message = YearlyProfitAndLoss(start_year_date=datetime.strptime(aggregated_yearly_pl_record[0], "%Y-%m-%d %H:%M:%S"),
                                                    end_year_date=datetime.strptime(aggregated_yearly_pl_record[1], "%Y-%m-%d %H:%M:%S"),
                                                    realised_pl=aggregated_yearly_pl_record[2])
            aggregated_yearly_pl_message_list.append(yearly_pl_message)
            
        for aggregated_yearly_pl_message in aggregated_yearly_pl_message_list:
            discord_client.send_message_by_list_with_response([aggregated_yearly_pl_message], DiscordChannel.YEARLY_PROFIT_AND_LOSS)
            add_sent_aggregated_yearly_pl_records(connector=sqlite_connector,
                                                  message_list=[aggregated_yearly_pl_message])

        