import datetime
from oracledb import Cursor

from sql.execute_query_impl import ExecuteQueryImpl

from constant.query.oracle_query import OracleQuery

def get_previous_day_top_gainer_list(connector, pct_change: float, start_date: datetime, end_date: datetime) -> bool:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.GET_TOP_GAINER_QUERY.value, **params)
        result = cursor.fetchall()
        return result

    exec = type(
        "CountTopGainerMessage", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    params = dict(percentage=pct_change, start_date=start_date, end_date=end_date)
    result = connector.execute_in_transaction(exec, params)
    
    return result

   
