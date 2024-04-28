from datetime import datetime

from oracledb import Cursor

from sql.execute_query_impl import ExecuteQueryImpl

from constant.query.oracle_query import OracleQuery
from constant.broker import Broker

def check_if_pattern_analysis_message_sent(connector, ticker: str, hit_scanner_datetime: datetime, scan_pattern: str, bar_size: str) -> bool:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.COUNT_PATTERN_ANALYSIS_MESSAGE_QUERY.value, **params)
        result = cursor.fetchone()
        no_of_result = result[0]
        return no_of_result

    exec = type(
        "CountPatternAnalysisMessage",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )
    
    params = dict(ticker=ticker, hit_scanner_datetime=hit_scanner_datetime, scan_pattern=scan_pattern, bar_size=bar_size)
    no_of_result = connector.execute_in_transaction(exec, params)
    
    if no_of_result == 1:
        return True
    else:
        return False

def add_sent_pattern_analysis_message_record(connector, param_list: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.ADD_PATTERN_ANALYSIS_QUERY.value, params)
    
    exec = type(
        "ExecBatchPatternAnalysisInsertion",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )
    
    connector.execute_in_transaction(exec, param_list)

def delete_all_sent_pattern_analysis_message_record(connector) -> int:
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.CLEAN_PATTERN_ANALYSIS_QUERY.value, params)
    
    exec = type(
        "ExecCleanPatternAnalysis",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )

    connector.execute_in_transaction(exec)

def check_if_trade_summary_message_sent(connector, ticker: str, acquired_date: datetime, sold_date: datetime, trading_platform: Broker) -> bool:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.COUNT_TRADE_SUMMARY_MESSAGE_QUERY.value, **params)
        result = cursor.fetchone()
        no_of_result = result[0]
        return no_of_result
    
    exec = type(
        "CountTradeSummaryMessage",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )
    
    params = dict(acquired_date=acquired_date, sold_date=sold_date, trading_platform=trading_platform)
    no_of_result = connector.execute_in_transaction(exec, params)
    
    if no_of_result == 1:
        return True
    else:
        return False

def add_sent_trade_summary_message_record(connector, param_list: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.ADD_PATTERN_ANALYSIS_QUERY.value, params)
    
    exec = type(
        "ExecBatchPatternAnalysisInsertion",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )
    
    connector.execute_in_transaction(exec, param_list)
    
def delete_all_sent_trade_summary_message_record(connector) -> int:
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.CLEAN_TRADE_SUMMARY_MESSAGE_QUERY.value, params)
    
    exec = type(
        "ExecCleanPatternAnalysis",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )

    connector.execute_in_transaction(exec)

# def check_if_daily_realised_pl_message_sent(connector, settle_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_DAILY_REALISED_PL_MESSAGE_EXIST_QUERY.value, (settle_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True
    
# def add_sent_daily_realised_pl_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_daily_realised_pl_message_record_list = []
#     for message in message_list:
#         add_daily_realised_pl_message_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                           message.realised_pl, 
#                                                           message.trading_platform.value))
    
#     cursor.executemany(OracleQuery.ADD_DAILY_REALISED_PL_MESSAGE_QUERY.value, add_daily_realised_pl_message_record_list)
#     cursor.connection.commit()
    
# def delete_all_sent_daily_realised_pl_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_DAILY_REALISED_PL_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_monthly_realised_pl_message_sent(connector, start_month_date: datetime, end_month_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_MONTHLY_REALISED_PL_MESSAGE_EXIST_QUERY.value, (start_month_date.strftime('%Y-%m-%d %H:%M:%S'), end_month_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True

# def add_sent_monthly_realised_pl_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_monthly_realised_pl_message_record_list = []
#     for message in message_list:
#         add_monthly_realised_pl_message_record_list.append((message.start_month_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                             message.end_month_date.strftime('%Y-%m-%d %H:%M:%S'),
#                                                             message.realised_pl,
#                                                             message.trading_platform.value))
    
#     cursor.executemany(OracleQuery.ADD_MONTHLY_REALISED_PL_MESSAGE_QUERY.value, add_monthly_realised_pl_message_record_list)
#     cursor.connection.commit()
    
# def delete_all_sent_monthly_realised_pl_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_MONTHLY_REALISED_PL_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_yearly_realised_pl_message_sent(connector, start_year_date: datetime, end_year_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_YEARLY_REALISED_PL_MESSAGE_EXIST_QUERY.value, (start_year_date.strftime('%Y-%m-%d %H:%M:%S'), end_year_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True

# def add_sent_yearly_realised_pl_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_yearly_realised_pl_message_record_list = []
#     for message in message_list:
#         add_yearly_realised_pl_message_record_list.append((message.start_year_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                             message.end_year_date.strftime('%Y-%m-%d %H:%M:%S'),
#                                                             message.realised_pl,
#                                                             message.trading_platform.value))
    
#     cursor.executemany(OracleQuery.ADD_YEARLY_REALISED_PL_MESSAGE_QUERY.value, add_yearly_realised_pl_message_record_list)
#     cursor.connection.commit()
    
# def delete_all_sent_yearly_realised_pl_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_YEARLY_REALISED_PL_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_month_to_date_realised_pl_message_sent(connector, settle_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_MONTH_TO_DATE_REALISED_PL_MESSAGE_EXIST_QUERY.value, (settle_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True

# def add_sent_month_to_date_realised_pl_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_month_to_date_realised_pl_message_record_list = []
#     for message in message_list:
#         add_month_to_date_realised_pl_message_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                   message.realised_pl,
#                                                                   message.trading_platform.value))
    
#     cursor.executemany(OracleQuery.ADD_MONTH_TO_DATE_REALISED_PL_MESSAGE_QUERY.value, add_month_to_date_realised_pl_message_record_list)
#     cursor.connection.commit()
    
# def delete_all_sent_month_to_date_realised_pl_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_MONTH_TO_DATE_REALISED_PL_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_year_to_date_realised_pl_message_sent(connector, settle_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_YEAR_TO_DATE_REALISED_PL_MESSAGE_EXIST_QUERY.value, (settle_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True

# def add_sent_year_to_date_realised_pl_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_year_to_date_realised_pl_message_record_list = []
#     for message in message_list:
#         add_year_to_date_realised_pl_message_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                   message.realised_pl,
#                                                                   message.trading_platform.value))
    
#     cursor.executemany(OracleQuery.ADD_YEAR_TO_DATE_REALISED_PL_MESSAGE_QUERY.value, add_year_to_date_realised_pl_message_record_list)
#     cursor.connection.commit()
    
# def delete_all_sent_year_to_date_realised_pl_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_YEAR_TO_DATE_REALISED_PL_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_weekly_realised_pl_message_sent(connector, start_week_date: str, end_week_date:str, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_WEEKLY_REALISED_PL_MESSAGE_EXIST_QUERY.value, (start_week_date.strftime('%Y-%m-%d %H:%M:%S'), end_week_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True

# def add_sent_weekly_realised_pl_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_weekly_realised_pl_message_record_list = []
#     for message in message_list:
#         add_weekly_realised_pl_message_record_list.append((message.start_week_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                            message.end_week_date.strftime('%Y-%m-%d %H:%M:%S'),
#                                                            message.realised_pl,
#                                                            message.trading_platform.value))
    
#     cursor.executemany(OracleQuery.ADD_WEEKLY_REALISED_PL_MESSAGE_QUERY.value, add_weekly_realised_pl_message_record_list)
#     cursor.connection.commit()
    
# def delete_all_sent_weekly_realised_pl_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_WEEKLY_REALISED_PL_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_interest_history_message_sent(connector, settle_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_INTEREST_HISTORY_MESSAGE_EXIST_QUERY.value, (settle_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True
    
# def add_sent_interest_history_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_interest_message_record_list = []
#     for message in message_list:
#         add_interest_message_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.interest_value, 
#                                                  message.paid_by.value))
    
#     cursor.executemany(OracleQuery.ADD_INTEREST_HISTORY_MESSAGE_QUERY.value, add_interest_message_record_list)
#     cursor.connection.commit()

# def delete_all_sent_interest_history_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_INTEREST_HISTORY_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def check_if_imported_pl_file_message_sent(connector, type: Broker, start_date: datetime, end_date: datetime, filename: str) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_IMPORT_PL_FILE_MESSAGE_EXIST_QUERY.value, (start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), filename, type))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True
    
# def add_imported_pl_file_record(connector, start_date: datetime, end_date: datetime, filename: str, type: Broker):
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.ADD_IMPORT_PL_FILE_MESSAGE_QUERY.value, (start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), filename, type.value))
#     cursor.connection.commit()

# def delete_all_sent_imported_pl_file_message_record(connector) -> int:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_IMPORT_PL_FILE_MESSAGE_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def get_all_aggregated_daily_pl_records(connector) -> list:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.GET_SEND_AGGREGATED_DAILY_REALISED_PL_QUERY.value)
    
#     rows = cursor.fetchall()
#     if rows is None:
#         return []
#     else:
#         return rows
    
# def delete_all_aggregated_daily_pl_records(connector) -> None:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_AGGREGATED_DAILY_REALISED_PL_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def get_all_aggregated_weekly_pl_records(connector) -> list:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.GET_SEND_AGGREGATED_WEEKLY_REALISED_PL_QUERY.value)
    
#     rows = cursor.fetchall()
#     if rows is None:
#         return []
#     else:
#         return rows
    
# def delete_all_aggregated_weekly_pl_records(connector) -> None:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_AGGREGATED_WEEKLY_REALISED_PL_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount
    
# def get_all_aggregated_month_to_date_pl_records(connector) -> list:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.GET_SEND_AGGREGATED_MONTH_TO_DATE_REALISED_PL_QUERY.value)
    
#     rows = cursor.fetchall()
#     if rows is None:
#         return []
#     else:
#         return rows
    
# def delete_all_aggregated_month_to_date_pl_records(connector) -> None:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_AGGREGATED_MONTH_TO_DATE_REALISED_PL_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount
    
# def get_all_aggregated_year_to_date_pl_records(connector) -> list:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.GET_SEND_AGGREGATED_YEAR_TO_DATE_REALISED_PL_QUERY.value)
    
#     rows = cursor.fetchall()
#     if rows is None:
#         return []
#     else:
#         return rows
    
# def delete_all_aggregated_year_to_date_pl_records(connector) -> None:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_AGGREGATED_YEAR_TO_DATE_REALISED_PL_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount
    
# def get_all_aggregated_monthly_pl_records(connector) -> list:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.GET_SEND_AGGREGATED_MONTHLY_REALISED_PL_EXIST_QUERY.value)
    
#     rows = cursor.fetchall()
#     if rows is None:
#         return []
#     else:
#         return rows
    
# def delete_all_aggregated_monthly_pl_records(connector) -> None:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_AGGREGATED_MONTHLY_REALISED_PL_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount
    
# def get_all_aggregated_yearly_pl_records(connector) -> list:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.GET_SEND_AGGREGATED_YEARLY_REALISED_PL_EXIST_QUERY.value)
    
#     rows = cursor.fetchall()
#     if rows is None:
#         return []
#     else:
#         return rows
    
# def delete_all_aggregated_yearly_pl_records(connector) -> None:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.DELETE_ALL_AGGREGATED_YEARLY_REALISED_PL_QUERY.value)
#     cursor.connection.commit()
#     return cursor.rowcount

# def add_sent_aggregated_daily_pl_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_send_aggregated_daily_pl_message_record_list = []
#     for message in message_list:
#         add_send_aggregated_daily_pl_message_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                  message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_DAILY_REALISED_PL_QUERY.value, add_send_aggregated_daily_pl_message_record_list)
#     cursor.connection.commit()
    
# def add_sent_aggregated_weekly_pl_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_send_aggregated_weekly_pl_message_record_list = []
#     for message in message_list:
#         add_send_aggregated_weekly_pl_message_record_list.append((message.start_week_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                   message.end_week_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                   message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_WEEKLY_REALISED_PL_QUERY.value, add_send_aggregated_weekly_pl_message_record_list)
#     cursor.connection.commit()
    
# def add_sent_aggregated_month_to_date_pl_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_send_aggregated_month_to_date_pl_message_record_list = []
#     for message in message_list:
#         add_send_aggregated_month_to_date_pl_message_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                          message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_MONTH_TO_DATE_REALISED_PL_QUERY.value, add_send_aggregated_month_to_date_pl_message_record_list)
#     cursor.connection.commit()
    
# def add_sent_aggregated_year_to_date_pl_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_send_aggregated_year_to_date_pl_record_list = []
#     for message in message_list:
#         add_send_aggregated_year_to_date_pl_record_list.append((message.settle_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                 message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_YEAR_TO_DATE_REALISED_PL_QUERY.value, add_send_aggregated_year_to_date_pl_record_list)
#     cursor.connection.commit()
    
# def add_sent_aggregated_monthly_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_send_aggregated_monthly_message_record_list = []
#     for message in message_list:
#         add_send_aggregated_monthly_message_record_list.append((message.start_month_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                 message.end_month_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                                 message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_MONTHLY_REALISED_PL_QUERY.value, add_send_aggregated_monthly_message_record_list)
#     cursor.connection.commit()
    
# def add_sent_aggregated_yearly_pl_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_interest_message_record_list = []
#     for message in message_list:
#         add_interest_message_record_list.append((message.start_year_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.end_year_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_YEARLY_REALISED_PL_QUERY.value, add_interest_message_record_list)
#     cursor.connection.commit()
    
    
# def add_sent_day_trade_summary_records(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_interest_message_record_list = []
#     for message in message_list:
#         add_interest_message_record_list.append((message.start_year_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.end_year_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.realised_pl))
    
#     cursor.executemany(OracleQuery.ADD_AGGREGATED_YEARLY_REALISED_PL_QUERY.value, add_interest_message_record_list)
#     cursor.connection.commit()
    
# def check_if_trade_summary_message_sent(connector, ticker: str, acquired_date: datetime, sold_date: datetime, trading_platform: Broker) -> bool:
#     cursor = connector.cursor
#     cursor.execute(OracleQuery.CHECK_TRADE_SUMMARY_MESSAGE_EXIST_QUERY.value, (ticker, acquired_date.strftime('%Y-%m-%d %H:%M:%S'), sold_date.strftime('%Y-%m-%d %H:%M:%S'), trading_platform.value))
    
#     row = cursor.fetchone()
#     if row is None:
#         return False
#     else:
#         return True
    
# def add_sent_trade_summary_message_record(connector, message_list: list):
#     cursor = connector.cursor
    
#     add_trade_summary_message_record_list = []
#     for message in message_list:
#         add_trade_summary_message_record_list.append((message.ticker,
#                                                  message.acquired_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.sold_date.strftime('%Y-%m-%d %H:%M:%S'), 
#                                                  message.avg_entry_price,
#                                                  message.avg_exit_price,
#                                                  message.realised_pl,
#                                                  message.realised_pl_percent,
#                                                  None,
#                                                  None,
#                                                  None,
#                                                  None,
#                                                  None,
#                                                  None,
#                                                  message.trading_platform))
    
#     cursor.executemany(OracleQuery.ADD_TRADE_SUMMARY_MESSAGE_QUERY.value, add_trade_summary_message_record_list)
#     cursor.connection.commit()