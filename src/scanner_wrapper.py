import time
import threading
from typing import Callable

from aiohttp import ClientError
from requests import HTTPError, RequestException

from utils.common.config_util import get_config
from utils.common.datetime_util import is_within_trading_day_and_hours
from utils.logger import Logger

logger = Logger()

# Scanner Idle Refresh Time
SCANNER_IDLE_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_IDLE_REFRESH_INTERVAL')

client_portal_connection_failed = False

class ScannerWrapper(threading.Thread):
    def __init__(self, scanner_name: str, scan: Callable, thread_name: str):
        threading.Thread.__init__(self, name=thread_name)
        self.__scanner_name = scanner_name
        self.__scan = scan
        self.exc = None

    # https://www.geeksforgeeks.org/handling-a-threads-exception-in-the-caller-thread-in-python/
    def join(self):
        threading.Thread.join(self)

        if self.exc:
            raise self.exc

    def run(self):
        global client_portal_connection_failed
        self.exc = None 
        
        while True: 
            start_scan = is_within_trading_day_and_hours()

            if start_scan:
                break
            else:
                if idle_message:
                    logger.log_debug_msg(idle_message, with_std_out=True)
                else:
                    idle_message = f'{threading.current_thread().name} is idle until valid trading weekday and time'

                time.sleep(SCANNER_IDLE_REFRESH_INTERVAL)
                
        while True and not client_portal_connection_failed:
            try:
                start_time = time.time()
                self.__scan()
                logger.log_debug_msg(f'{self.__scanner_name} scan time: {time.time() - start_time}', with_std_out=True)
            except (RequestException, ClientError, HTTPError) as connection_exception:
                client_portal_connection_failed = True
                self.exc = connection_exception
                break
            except Exception as e:
                self.exc = e
                break
