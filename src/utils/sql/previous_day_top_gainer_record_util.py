import datetime
from oracledb import Cursor

from sql.oracle_connector import execute_in_transaction
from sql.execute_query_impl import ExecuteQueryImpl

from constant.query.oracle_query import OracleQuery

def get_previous_day_top_gainer_list(pct_change: float, start_datetime: datetime.datetime, end_datetime: datetime.datetime) -> list:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.GET_TOP_GAINER_QUERY.value, **params)
        result = cursor.fetchall()
        return result

    exec = type(
        "GetPreviousDayTopGainerList", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    params = dict(percentage=pct_change, start_datetime=start_datetime, end_datetime=end_datetime)
    result = execute_in_transaction(exec, params)
    
    return result

   
