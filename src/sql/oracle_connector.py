import oracledb

from sql.execute_query_impl import ExecuteQueryImpl

from utils.config_util import get_config
from utils.logger import Logger

logger = Logger()

HOSTNAME = get_config('ORACLE_DB', 'HOSTNAME')
PORT = get_config('ORACLE_DB', 'PORT')
SID = get_config('ORACLE_DB', 'SID')
USERNAME = get_config('ORACLE_DB', 'USERNAME')
PASSWORD = get_config('ORACLE_DB', 'PASSWORD')

LOGIN_CREDENTIALS = dict(host=HOSTNAME, 
                         port=PORT, 
                         sid=SID,
                         user=USERNAME, 
                         password=PASSWORD)

def execute_in_transaction(execute_query: ExecuteQueryImpl, params):
    connection = None
    cursor = None
    result = None
        
    try:
        connection = oracledb.connect(**LOGIN_CREDENTIALS)
        cursor = connection.cursor()
        result = execute_query.execute(cursor, params)
            
        connection.commit()
    except oracledb.Error as e:
        if connection:
            connection.rollback()
            logger.log_error_msg(f'Rollback due to error, {e}')
                
            logger.log_error_msg(f'Oracle SQL error, {e}')
            raise e
    finally:
        if connection is not None:
            if cursor is not None:
                cursor.close()
    
            connection.close()
            
    return result
    