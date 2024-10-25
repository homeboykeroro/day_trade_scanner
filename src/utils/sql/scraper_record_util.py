import datetime

from oracledb import Cursor
from sql.execute_query_impl import ExecuteQueryImpl
from sql.oracle_connector import execute_in_transaction

from constant.sql.oracle_query import OracleQuery

from utils.logger import Logger

logger = Logger()

def check_if_top_gainer_added(ticker: str, scan_date: datetime.datetime) -> bool:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.COUNT_TOP_GAINER_TICKER_QUERY.value, **params)
        result = cursor.fetchone()
        no_of_result = result[0]
        return no_of_result
    
    #https://mihfazhillah.medium.com/anonymous-class-in-python-39e42140db94
    exec = type(
        "ExecCheckIfTopGainerAddedQuery", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    no_of_result = execute_in_transaction(exec, dict(ticker=ticker, scan_date=scan_date))
    
    if no_of_result == 1:
        return True
    else:
        return False

def add_top_gainer_record(params: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.ADD_TOP_GAINER_QUERY.value, params)
    
    exec = type(
        "ExecBatchTopGainerInsertion", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    execute_in_transaction(exec, params)