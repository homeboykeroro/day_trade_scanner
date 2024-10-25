import datetime
import time
from oracledb import Cursor

from utils.logger import Logger

from sql.oracle_connector import execute_in_transaction
from sql.execute_query_impl import ExecuteQueryImpl

from constant.query.oracle_query import OracleQuery

logger = Logger()

def update_api_endpoint_lock(params: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.UPDATE_API_ENDPOINT_LOCK_QUERY.value, params)

    exec = type(
        "UpdateApiEndpointLockRecord", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    execute_in_transaction(exec, params)

def get_locked_api_endpoint(locked_by: str) -> list:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.GET_API_ENDPOINT_LOCK_BY_LOCKED_BY_QUERY.value, **params)
        result = cursor.fetchall()
        return result

    exec = type(
        "GetLockedApiEndpoint", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    params = dict(locked_by=locked_by)
    result = execute_in_transaction(exec, params)
    
    return result

def check_api_endpoint_locked(endpoint: str) -> bool:
    start_time = time.time()
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
    
    if result:
        is_locked = result[1]
        locked_by = result[2]
        lock_datetime = result[3]
    else:
        raise Exception(f'Get API endpoint {endpoint} record error, no record found')
    
    logger.log_debug_msg(f'{endpoint} lock status: {is_locked}, locked by: {locked_by}, lock datetime: {lock_datetime}')
    logger.log_debug_msg(f'Check {endpoint} endpoint lock time: {time.time() - start_time}')
        
    return is_locked

   
