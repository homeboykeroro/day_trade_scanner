import threading
import time
from typing import Callable
from aiohttp import ClientError
import oracledb
from requests import HTTPError, RequestException

from datasource.ib_connector import IBConnector

from module.discord_chatbot_client import DiscordChatBotClient

from utils.logger import Logger

logger = Logger()
#reauthentication_lock = threading.Lock()

#class ScannerThreadWrapper(threading.Thread):
class ScannerThreadWrapper():
    def __init__(self, scan: Callable, 
                 name: str,
                 ib_connector: IBConnector,
                 discord_client: DiscordChatBotClient):
        self.exc = None
        
        self.__scan = scan
        self.__ib_connector = ib_connector
        self.__discord_client = discord_client
        #super().__init__(name=name)
        self.__name = name
            
    #def run(self) -> None:  
    def start(self) -> None:  
        try:
            self.__scan(self.__ib_connector, self.__discord_client)
            #logger.log_debug_msg(f'{self.__name} scan finished, sleep 10 seconds')
            #time.sleep(5)
        except (RequestException, ClientError, HTTPError) as connection_exception:
            self.exc = connection_exception
            raise connection_exception
        except oracledb.Error as oracle_connection_exception:
            self.exc = oracle_connection_exception
            raise oracle_connection_exception
        except Exception as exception:
            self.exc = exception
            raise exception
            
    # def join(self):
    #     threading.Thread.join(self)
        
    #     if self.exc:
    #         raise self.exc