from datetime import datetime

from oracledb import Cursor

from sql.oracle_connector import execute_in_transaction
from sql.execute_query_impl import ExecuteQueryImpl

from constant.query.oracle_query import OracleQuery
from constant.broker import Broker

def check_if_pattern_analysis_message_sent(ticker: str, hit_scanner_datetime: datetime, scan_pattern: str, bar_size: str) -> bool:
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
    no_of_result = execute_in_transaction(exec, params)
    
    if no_of_result == 1:
        return True
    else:
        return False

def add_sent_pattern_analysis_message_record(param_list: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.ADD_PATTERN_ANALYSIS_QUERY.value, params)
    
    exec = type(
        "ExecBatchPatternAnalysisInsertion",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )
    
    execute_in_transaction(exec, param_list)

def delete_all_sent_pattern_analysis_message_record() -> int:
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.CLEAN_PATTERN_ANALYSIS_QUERY.value, params)
    
    exec = type(
        "ExecCleanPatternAnalysis",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )

    execute_in_transaction(exec)

def check_if_trade_summary_message_sent(ticker: str, acquired_date: datetime, sold_date: datetime, trading_platform: Broker) -> bool:
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
    no_of_result = execute_in_transaction(exec, params)
    
    if no_of_result == 1:
        return True
    else:
        return False

def add_sent_trade_summary_message_record(param_list: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.ADD_PATTERN_ANALYSIS_QUERY.value, params)
    
    exec = type(
        "ExecBatchPatternAnalysisInsertion",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )
    
    execute_in_transaction(exec, param_list)
    
def delete_all_sent_trade_summary_message_record() -> int:
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.CLEAN_TRADE_SUMMARY_MESSAGE_QUERY.value, params)
    
    exec = type(
        "ExecCleanPatternAnalysis",
        (ExecuteQueryImpl,),
        {
            "execute": execute
        }
    )

    execute_in_transaction(exec)
