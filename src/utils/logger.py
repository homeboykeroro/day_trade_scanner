from datetime import datetime
import logging
import os
import pandas as pd

from utils.config_util import get_config

LOGGER_DIR = get_config('LOGGER', 'PATH')
LOG_DATAFRAME = get_config('LOGGER', 'LOG_DATAFRAME')

class Logger:
    def __init__(self):
        self.__logger = self.__get_logger()
        
    def log_df_debug_msg(self, msg):
        self.__logger.debug(msg)

    def log_debug_msg(self, msg, with_log_file: bool = True, with_std_out: bool = False):
        if with_std_out:
            print(msg)
        
        if with_log_file:
            if (isinstance(msg, pd.DataFrame) and LOG_DATAFRAME) or isinstance(msg, str):
                self.__logger.debug(msg)
            
    def log_error_msg(self, msg: str, with_log_file: bool = True, with_std_out: bool = False):
        if with_std_out:
            print(msg)
        
        if with_log_file:
            self.__logger.exception(msg)

    def __get_logger(self, name: str = 'root',
                   log_parent_directory: str = LOGGER_DIR,
                   level: int = logging.DEBUG,
                   display_format: str = '\r(%(threadName)s) %(asctime)s - %(message)s (%(levelname)s)',
                   date_format: str = '%m/%d/%Y %I:%M:%S %p'):
        log_date = datetime.now().strftime('%Y%m%d')
        log_filename = 'client_portal_scanner_log_' + log_date + '.txt'
        log_dir = log_parent_directory + "/" + log_filename
        if not os.path.exists(os.path.dirname(log_dir)) and os.path.dirname(log_dir):
            os.makedirs(os.path.dirname(log_dir))

        logger = logging.getLogger(name)
        handler = logging.FileHandler(log_dir)
        logger.setLevel(level)

        if not len(logger.handlers):
            formatter = logging.Formatter(display_format, datefmt=date_format)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger