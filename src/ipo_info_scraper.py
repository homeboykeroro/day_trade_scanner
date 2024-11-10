import re
import threading

from constant.scanner.scanner_thread_name import ScannerThreadName

threading.current_thread().name = ScannerThreadName.IPO_INFO_SCRAPER.value

import asyncio
from sqlite3 import Cursor
import aiohttp
from datetime import datetime
import os
import time
from bs4 import BeautifulSoup
import requests
from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC

from module.discord_chatbot_client import DiscordChatBotClient

from sql.execute_query_impl import ExecuteQueryImpl
from sql.oracle_connector import execute_in_transaction

from utils.logger import Logger
from utils.common.config_util import get_config

from model.discord.discord_message import DiscordMessage
from model.discord.ipo_message import IPOMessage

from constant.query.oracle_query import OracleQuery
from constant.discord.discord_channel import DiscordChannel

IPO_LIST_LINK = 'https://www.nasdaq.com/market-activity/ipos'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}

session = requests.Session()
logger = Logger()

# Chatbot
ipo_info_scraper_chatbot = DiscordChatBotClient()

# Refresh Time
REFRESH_INTERVAL = get_config('IPO_INFO_SCRAPER', 'REFRESH_INTERVAL')
SELENIUM_DRIVER_PATH = get_config('SYS_PARAM', 'SELENIUM_DRIVER_PATH')

async def fetch(session: aiohttp.ClientSession, link: str):
    response = None
    
    try:
        async with session.get(link, ssl=False, headers=HEADERS) as response:
            response = await response
            return response
    except Exception as e:
        status_code = 500 if not hasattr(response, 'status') else response.status
        logger.log_error_msg(f'Error during GET request to {link}, Cause: {e}, Status code: {status_code}')

async def get_ipo_info(link_list: list):
    result_dict = {'response_list': [], 'error_response_list': []}
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for link in link_list:
            tasks.append(asyncio.create_task(fetch(session, link)))
        
        response_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for response in response_list:
            isError = isinstance(response, Exception)
            if isError or 'errorMsg' in response:
                result_dict['error_response_list'].append(response)
            else:
                result_dict['response_list'].append(response)
                    
    return result_dict

def delete_ipo_record(params: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.DELETE_IPO_QUERY.value, params)

    exec = type(
        "DeleteIPORecord", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    execute_in_transaction(exec, params)

def check_if_ipo_added(ticker: str, ipo_datetime: datetime) -> bool:
    def execute(cursor: Cursor, params):
        cursor.execute(OracleQuery.GET_IPO_QUERY.value, **params)
        result = cursor.fetchall()
        return result
    
    #https://mihfazhillah.medium.com/anonymous-class-in-python-39e42140db94
    exec = type(
        "ExecCheckIfIPOAddedQuery", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    result = execute_in_transaction(exec, dict(ticker=ticker))
    
    if len(result) == 1:
        if result[5] != ipo_datetime:
            logger.log_debug_msg(f'{ticker} IPO date require updates', with_std_out=True)
            delete_ipo_record([dict(ticker)])
            return False
        
        return True
    else:
        return False

def add_ipo_record(params: list):
    def execute(cursor: Cursor, params):
        cursor.executemany(OracleQuery.ADD_IPO_LIST_QUERY.value, params)
    
    exec = type(
        "ExecBatchIPOInsertion", # the name
        (ExecuteQueryImpl,), # base classess
        {
            "execute": execute
        }
    )
    
    execute_in_transaction(exec, params)

def send_message(message):
    response = ipo_info_scraper_chatbot.send_message_by_list_with_response(message_list=[message], channel_type=DiscordChannel.IPO_LIST)
    
    if not hasattr(response, 'embeds'):
        ipo_info_scraper_chatbot.send_message(DiscordMessage(content=f'Failed to send message {str(response)} to {DiscordChannel.IPO_LIST.value}'), DiscordChannel.CHATBOT_ERROR_LOG)
    else:  
        jump_url = response.jump_url
        readout_msg = message.readout_msg
        ticker = message.ticker
        notification_message = DiscordMessage(ticker=ticker, jump_url=jump_url, content=readout_msg)
        ipo_info_scraper_chatbot.send_message(message_list=[notification_message], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)

def scrap():
    # Chatbot Token
    IPO_INFO_SCRAPER_CHATBOT_TOKEN = os.environ['DISCORD_IPO_INFO_SCRAPER_CHATBOT_TOKEN']
    CHATBOT_THREAD_NAME = 'ipo_info_scraper_chatbot_thread'

    ipo_info_scraper_chatbot.run_chatbot(CHATBOT_THREAD_NAME, IPO_INFO_SCRAPER_CHATBOT_TOKEN)
    ipo_info_scraper_chatbot.send_message_by_list_with_response([DiscordMessage(content='Starts scanner')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
    
    start_time = time.time()
    
    try:
        driver = webdriver.Chrome(SELENIUM_DRIVER_PATH) 
        scrap_star_time = time.time()
        driver.get('https://www.nasdaq.com/market-activity/ipos')
        WebDriverWait(driver, 30).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        page_content = driver.page_source
        driver.quit()
        logger.log_debug_msg(f'Get IPO list response time: {time.time() - scrap_star_time} seconds')
        
        save_ipo_list = []
        ipo_info_dict = {}
        info_link_list = []
        soup = BeautifulSoup(page_content, 'lxml')
        row_list = soup.select('div[class$="-ipo-calendar__priced_table"] .table-row[part="table-row"][role="row"]')
        logger.log_debug_msg(f'Number of IPO: {len(row_list)}')
            
        for row in row_list:
            column_list = row.find_all('div[part="table-cell"][role="cell"]')

            ticker = column_list[0].text
            
            if not re.match('^[A-Z]{1,4}$', ticker): 
                logger.log_debug_msg(f'Exclude {ticker} from IPO list', with_std_out=True)
                continue
            
            company_name = column_list[1].text
            exchange_name = column_list[2].text
            offering_price = float(column_list[3].text.replace(',', ''))
            offering_shares = float(column_list[4].text.replace(',', ''))
            ipo_date = datetime.strptime(column_list[5].text, "%m/%d/%Y")
            offering_amount = float(column_list[6].text.replace(',', ''))

            ipo_info_dict[ticker] = dict(company_name=company_name, 
                                         exchange_name=exchange_name,
                                         offering_price=offering_price,
                                         offering_shares=offering_shares,
                                         offering_amount=offering_amount,
                                         ipo_date=ipo_date)

            info_link = column_list[0].select_one('a').get('href')
            info_link_list.append(info_link)

        ipo_details_retrieval_start_time = time.time()
        ipo_details_response = asyncio.run(get_ipo_info(info_link_list))
        ipo_details_list = ipo_details_response.get('response_list')
        ipo_details_error_list = ipo_details_response.get('error_response_list')
        logger.log_debug_msg(f'Get IPO details time: {time.time() - ipo_details_retrieval_start_time} seconds')
            
        if ipo_details_error_list:
            logger.log_error_msg(f'IPO details retrieval error response list: {ipo_details_error_list}')

        for details_response in ipo_details_list:
            ipo_details_content = details_response.text
            ipo_details_soup = BeautifulSoup(ipo_details_content, 'lxml')
            details_dict = {}

            details = ipo_details_soup.find_all('div.overview-container div[class="insert-data"] table tr')
            description = ipo_details_soup.select_one('div.description-data span').get_text()
            details_dict['description'] = description

            for detail in details:
                field = detail.select_one('th').get_text()
                value = detail.select_one('td').get_text()

                if field == 'Proposed Symbol':
                    details_dict['ticker'] = value
                elif field == 'Company Address':
                    details_dict['address'] = value
                elif field == 'Company Website':
                    details_dict['official_website'] = value
                elif field == 'Employee':
                    details_dict['number_of_employee'] = value

            ipo_info_dict[details_dict['ticker']].update(details_dict)
            save_ipo_list.append([ipo_info_dict.get('ticker'),
                                  ipo_info_dict.get('company_name'),
                                  ipo_info_dict.get('offering_price'),
                                  ipo_info_dict.get('offering_shares'),
                                  ipo_info_dict.get('offering_amount'),
                                  ipo_info_dict.get('ipo_datetime'),
                                  ipo_info_dict.get('official_website'),
                                  ipo_info_dict.get('address'),
                                  ipo_info_dict.get('number_of_employee')])

            title = f'{ipo_info_dict.get("company_name")} Initial Public Offering at ({ipo_info_dict.get("ipo_datetime").strftime("%m/%d/%Y")})'
            readout_msg = title
                
            is_sent = check_if_ipo_added(ipo_info_dict.get('ticker'))
            if not is_sent:
                message = IPOMessage(title=title,
                                     readout_msg=readout_msg,
                                     offering_price=ipo_info_dict.get('offering_price'),
                                     offering_shares=ipo_info_dict.get('offering_shares'),
                                     offering_amount=ipo_info_dict.get('offering_amount'),
                                     description=ipo_info_dict.get('description'),
                                     company_website=ipo_info_dict.get('official_website'),
                                     ticker=ipo_info_dict.get('ticker'))
                send_message(message)
                add_ipo_record(save_ipo_list)
    except Exception as e:
        logger.log_error_msg(f'An error occurred while scarping IPO list: {e}', with_std_out=True)
    logger.log_debug_msg(f'IPO scraping time: {time.time() - start_time} seconds')

def run():
    while True: 
        scrap()
        time.sleep(REFRESH_INTERVAL)

if __name__ == '__main__':
    scrap()