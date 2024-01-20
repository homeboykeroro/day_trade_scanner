import sqlite3

from utils.logger import Logger
from exception.sqlite_connection_error import SqliteConnectionError

DISCORD_NOTIFICATION_DB_DIR = 'C:/Users/John/Downloads/Trade History/Scanner/stock_screener.db'

logger = Logger()

class SqliteConnector:
    def __init__(self):
        try:
            self.__connection = sqlite3.connect(DISCORD_NOTIFICATION_DB_DIR)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            logger.log_error_msg(f'SQLite connection creation error, cause: {e}')
            
            if self.__cursor:
                self.__cursor.close()
            
            if self.__connection:
                self.__connection.close()
                
            logger.log_error_msg('SQLite connection closed')
            raise SqliteConnectionError
    
    @property
    def connection(self):
        return self.__connection
    
    @connection.setter
    def connection(self, connection):
        self.__connection = connection
        
    @property
    def cursor(self):
        return self.__cursor
    
    @cursor.setter
    def cursor(self, cursor):
        self.__cursor = cursor
    
    def execute_with_batch(self, query: str, data: list) -> None:
        self.__cursor.executemany(query, data)
        self.__connection.commit()
    
    def execute_query(self, query: str) -> list:
        self.__cursor.execute(query)
        self.__connection.commit()
        return self.__cursor.fetchall()
