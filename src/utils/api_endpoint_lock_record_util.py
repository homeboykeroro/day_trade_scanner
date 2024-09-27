import datetime
from oracledb import Cursor

from utils.logger import Logger

from sql.oracle_connector import execute_in_transaction
from sql.execute_query_impl import ExecuteQueryImpl

from constant.query.oracle_query import OracleQuery

logger = Logger()

def update_api_endpoint_lock(endpoint: str, is_locked: bool, locked_by: str, lock_datetime: datetime.datetime):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.UPDATE_API_ENDPOINT_LOCK_QUERY.value, params)

    exec = type(
        "UpdateApiEndpointLockRecord", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    params = dict(endpoint=endpoint, is_locked=is_locked, locked_by=locked_by, lock_datetime=lock_datetime)
    execute_in_transaction(exec, params)

def check_api_endpoint_locked(endpoint: str) -> bool:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.GET_API_ENDPOINT_LOCK_QUERY.value, **params)
        result = cursor.fetchone()
        return result

    exec = type(
        "GetApiEndpointLockRecord", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    params = dict(endpoint=endpoint)
    result = execute_in_transaction(exec, params)
    
    is_locked = result[1]
    locked_by = result[2]
    lock_datetime = result[3]
    
    logger.log_debug_msg(f'{endpoint} lock status: {is_locked}, locked by: {locked_by}, lock datetime: {lock_datetime}')
    
    return is_locked

   
