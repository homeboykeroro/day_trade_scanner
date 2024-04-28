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

MIN_CONNECTION_IN_POOL = get_config('ORACLE_DB', 'MIN_CONNECTION_IN_POOL')
MAX_CONNECTION_IN_POOL = get_config('ORACLE_DB', 'MAX_CONNECTION_IN_POOL')
POOL_CONNECTION_INCREMENT = get_config('ORACLE_DB', 'POOL_CONNECTION_INCREMENT')

LOGIN_CREDENTIALS = dict(host=HOSTNAME, 
                        port=PORT, 
                        sid=SID,
                        user=USERNAME, 
                        password=PASSWORD,
                        min=MIN_CONNECTION_IN_POOL,
                        max=MAX_CONNECTION_IN_POOL,
                        increment=POOL_CONNECTION_INCREMENT)

class OracleConnector:
    def __init__(self):
        #https://github.com/oracle/python-oracledb/discussions/159
        self.__pool = oracledb.create_pool(**LOGIN_CREDENTIALS)

    def __del__(self):
        logger.log_debug_msg('OracleConnector is being garbage collected')

    def execute_in_transaction(self, execute_query: ExecuteQueryImpl, params):
        connection = None
        cursor = None
        
        result = None
        
        try:
            connection = self.__pool.acquire()
            # logger.log_debug_msg(f'max life time: {self.__pool.max_lifetime_session}')
            # logger.log_debug_msg(f'timeout: {self.__pool.timeout}')
            # logger.log_debug_msg(f'number of conection acquired in pool: {self.__pool.busy}')
            # logger.log_debug_msg(f'addditional connection needs to be creataed: {self.__pool.increment}')
            # logger.log_debug_msg(f'number of conection opened in pool: {self.__pool.opened}')
            
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
            if self.__pool is not None:
                if connection is not None:
                    if cursor is not None:
                        cursor.close()
    
                    #self.__pool.release(connection)
                    connection.close()
            
        return result
    