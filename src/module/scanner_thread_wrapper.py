import threading
from typing import Callable
from aiohttp import ClientError
import oracledb
from requests import HTTPError, RequestException

from datasource.ib_connector import IBConnector

from module.discord_chatbot_client import DiscordChatBotClient

from utils.logger import Logger

logger = Logger()
reauthentication_lock = threading.Lock()

class ScannerThreadWrapper(threading.Thread):
    def __init__(self, scan: Callable, 
                 name: str,
                 ib_connector: IBConnector,
                 discord_client: DiscordChatBotClient):
        self.exc = None
        
        self.__scan = scan
        self.__ib_connector = ib_connector
        self.__discord_client = discord_client
        super().__init__(name=name)
            
    def run(self) -> None:  
        try:
            self.__scan(self.__ib_connector, self.__discord_client)
        except (RequestException, ClientError, HTTPError) as connection_exception:
            self.exc = connection_exception
            raise connection_exception
        except oracledb.Error as oracle_connection_exception:
            self.exc = oracle_connection_exception
            raise oracle_connection_exception
        except Exception as exception:
            self.exc = exception
            raise exception
            
    def join(self):
        threading.Thread.join(self)
        
        if self.exc:
            raise self.exc