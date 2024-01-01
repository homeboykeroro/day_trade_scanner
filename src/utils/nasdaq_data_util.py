import re
import ftplib
import time

from utils.logger import Logger

logger = Logger()

NASDAQ_TRADER_FTP_LINK = 'ftp.nasdaqtrader.com'
NASDAQ_TRADER_FTP_LOGIN_USER = 'anonymous'

TICKER_FILE_LIST = ['nasdaqlisted.txt', 'otherlisted.txt']

def get_all_ticker_in_the_market():
    ticker_list = []
    start_time = time.time()
    
    try:
        ftp = ftplib.FTP(NASDAQ_TRADER_FTP_LINK)
        ftp.login(NASDAQ_TRADER_FTP_LOGIN_USER, '')
        ftp.cwd('SymbolDirectory')
        files_to_download = TICKER_FILE_LIST
        for file in files_to_download:
            with open(file, 'wb') as f:
                ftp.retrbinary(f'RETR {file}', f.write)
        ftp.quit()
    except Exception as download_ticker_list_exception:
        logger.log_error_msg(f'Error occurred while downloading listed stocks info from NASDAQ FTP: {download_ticker_list_exception}')
    
    files_to_read = TICKER_FILE_LIST
    regex = r'^[a-zA-Z]{1,4}$'
    
    try:
        for file in files_to_read:
            with open(file, 'r') as f:
                lines = f.readlines()
                for line in lines[1:]:  # Skip the header
                    fields = line.split('|')
                    symbol = fields[0]
                    is_test_issue = fields[3] if file == 'nasdaqlisted.txt' else fields[6]
                    is_etf = fields[6] if file == 'nasdaqlisted.txt' else fields[4]

                    if re.match(regex, symbol) and is_etf == 'N' and is_test_issue == 'N': 
                        ticker_list.append(symbol)
                    else:
                        logger.log_debug_msg(f'Exclude ticker of "{symbol}" from {file}, details: {line}')
    except Exception as read_ticker_list_file_exception:
        logger.log_error_msg(f'Error occurred while reading files: {read_ticker_list_file_exception}')
    
    logger.log_debug_msg(f'Get all ticker from nasdaq time: {time.time() - start_time}')
    logger.log_debug_msg(f'Ticker list size: {len(ticker_list)}')
    logger.log_debug_msg(f'Ticker list: {ticker_list}')
    
    return ticker_list
                    
                    