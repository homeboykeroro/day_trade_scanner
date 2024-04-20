from collections import OrderedDict
from datetime import datetime
import os
import re
import threading
import time
import traceback
import requests

from module.discord_chatbot_client import DiscordChatBotClient

from datasource.ib_connector import IBConnector
from datasource.pl.pl_report import PLReport
from datasource.pl.firstrade_pl_report import FirstradePLReport
from datasource.pl.ib_pl_report import IBPLReport
from sql.sqlite_connector import SqliteConnector

from model.discord.discord_message import DiscordMessage

from utils.discord_message_record_util import add_imported_pl_file_record, check_if_imported_pl_file_message_sent
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel
from constant.broker import Broker

logger = Logger()
session = requests.Session()

FIRSTRADE_FILE_DIR = 'C:/Users/John/Downloads/Trade History/Scanner/PL/Firstrade'
IB_FILE_DIR = 'C:/Users/John/Downloads/Trade History/Scanner/PL/IB'
CITI_FILE_DIR = ''
HSBC_FILE_DIR = ''
SCAN_INTERVAL = 300

class PLReportGenerator():
    def __init__(self, discord_client: DiscordChatBotClient):
        self.__discord_client = discord_client
        self.__ib_connector = IBConnector()
        self.__stop_thread = False
        
    def __scan_firstrade_trade_data_file_and_update(self):
        ft_settle_date_to_files_dict = {}
        
        try:
            for file in os.listdir(FIRSTRADE_FILE_DIR):
                ft_account_summary_report_name_match = re.search(r"account_summary_(\d{8})_(\d{8})\.pdf", file)
                ft_trade_and_realised_pl_report_name_match = re.search(r"FT_GainLoss_(\d{8})_(\d{8})\.csv", file)

                if ft_account_summary_report_name_match:
                    start_date_str, end_date_str = ft_account_summary_report_name_match.groups()
                    start_date = datetime.strptime(start_date_str, "%Y%m%d")
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")
                    
                    if (start_date, end_date) not in ft_settle_date_to_files_dict:
                        ft_settle_date_to_files_dict[(start_date, end_date)] = {}

                    ft_settle_date_to_files_dict[(start_date, end_date)]['account_summary'] = file

                if ft_trade_and_realised_pl_report_name_match:
                    start_date_str, end_date_str = ft_trade_and_realised_pl_report_name_match.groups()
                    start_date = datetime.strptime(start_date_str, "%Y%m%d")
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")
                    
                    if (start_date, end_date) not in ft_settle_date_to_files_dict:
                        ft_settle_date_to_files_dict[(start_date, end_date)] = {}

                    ft_settle_date_to_files_dict[(start_date, end_date)]['trade_and_realised_pl'] = file

            if ft_settle_date_to_files_dict:
                ordered_ft_settle_date_to_files_dict = OrderedDict(sorted(ft_settle_date_to_files_dict.items(), key=lambda t: t[0]))
                processing_file = ''
                
                for date_range, file_dict in ordered_ft_settle_date_to_files_dict.items():
                    account_nav_file = file_dict.get('account_summary')
                    trade_and_realised_pl_file = file_dict.get('trade_and_realised_pl')

                    if account_nav_file:
                        processing_file = account_nav_file
                        is_account_summary_file_imported = check_if_imported_pl_file_message_sent(connector=self.__sqlite_connector,
                                                                                                  type=Broker.FIRSTRADE,
                                                                                                  start_date=date_range[0],
                                                                                                  end_date=date_range[1],
                                                                                                  filename=account_nav_file)
                        if not is_account_summary_file_imported:
                            self.__firstrade_report.update_account_nav_value(os.path.join(FIRSTRADE_FILE_DIR, account_nav_file))
                            add_imported_pl_file_record(connector=self.__sqlite_connector,)

                    if trade_and_realised_pl_file:
                        processing_file = trade_and_realised_pl_file
                        is_trade_and_realised_pl_file_imported = check_if_imported_pl_file_message_sent(connector=self.__sqlite_connector,
                                                                                                        type=Broker.FIRSTRADE,
                                                                                                        start_date=date_range[0],
                                                                                                        end_date=date_range[1],
                                                                                                        filename=trade_and_realised_pl_file)
                        if not is_trade_and_realised_pl_file_imported:
                            self.__firstrade_report.update_realised_pl_and_trade_summary(os.path.join(FIRSTRADE_FILE_DIR, trade_and_realised_pl_file))
                            add_imported_pl_file_record(connector=self.__sqlite_connector,)
        except Exception as exception:
            raise Exception(f'Firstrade profit and loss report {processing_file} import failed')
        
    def __scan_ib_trade_data_file_and_update(self):
        settle_date_to_trade_data_file_dict = {}
        
        try:
            for file in os.listdir(IB_FILE_DIR):
                ib_report_with_date_range_found = re.search(r"^IB_GainLoss_(\d{8})_(\d{8})\.csv", file)

                if ib_report_with_date_range_found:
                    start_date_str, end_date_str = ib_report_with_date_range_found.groups()
                    start_date = datetime.strptime(start_date_str, "%Y%m%d")
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")

                    settle_date_to_trade_data_file_dict[(start_date, end_date)] = file

            if settle_date_to_trade_data_file_dict:
                ordred_settle_date_to_trade_data_file_dict = OrderedDict(sorted(settle_date_to_trade_data_file_dict.items(), key=lambda t: t[0]))

                for start_date_end_date_key, trade_data_file in ordred_settle_date_to_trade_data_file_dict.items():
                    is_trade_data_file_imported = check_if_imported_pl_file_message_sent(connector=self.__sqlite_connector,
                                                                                         type=Broker.IB,
                                                                                         start_date=start_date_end_date_key[0],
                                                                                         end_date=start_date_end_date_key[1],
                                                                                         filename=trade_data_file)
                    if not is_trade_data_file_imported:
                        self.__ib_report_.update_realised_pl_and_trade_summary(os.path.join(IB_FILE_DIR, trade_data_file))

                        add_imported_pl_file_record(connector=self.__sqlite_connector,
                                                    start_date=start_date_end_date_key[0], 
                                                    end_date=start_date_end_date_key[1],
                                                    filename=trade_data_file,
                                                    type=Broker.IB)
        except Exception as exception:
            raise Exception(f'IB profit and loss report {trade_data_file} import failed')
        
    def scan_files(self):
        self.__sqlite_connector = SqliteConnector()
        self.__firstrade_report = FirstradePLReport(self.__ib_connector, self.__sqlite_connector, self.__discord_client)
        self.__ib_report_ = IBPLReport(self.__ib_connector, self.__sqlite_connector, self.__discord_client)
        
        while not self.__stop_thread:
            try:
                self.__scan_firstrade_trade_data_file_and_update()
                self.__scan_ib_trade_data_file_and_update()
                PLReport.send_aggregated_messages(self.__sqlite_connector, self.__discord_client)
                time.sleep(SCAN_INTERVAL)
            except Exception as exception: # Must be 2000 or fewer in length
                self.__discord_client.send_message(DiscordMessage(content=exception), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                self.__discord_client.send_message(DiscordMessage(content=traceback.format_exc()), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
                logger.log_error_msg(f'Profit and loss report generation error, {exception}', with_std_out=True)
                time.sleep(10)
                self.__stop_thread = True
                
    def run_pl_report(self) -> None:
        report_generator_thread = threading.Thread(target=self.scan_files, name="pl_report_thread")
        report_generator_thread.start()
        