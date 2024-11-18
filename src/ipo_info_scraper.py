import re
import threading
import traceback

from constant.scanner.scanner_thread_name import ScannerThreadName

threading.current_thread().name = ScannerThreadName.IPO_INFO_SCRAPER.value

from sqlite3 import Cursor
from datetime import datetime
import os
import time
import requests
from selenium import webdriver 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.common.by import By 

from module.discord_chatbot_client import DiscordChatBotClient

from sql.execute_query_impl import ExecuteQueryImpl
from sql.oracle_connector import execute_in_transaction

from utils.logger import Logger
from utils.common.config_util import get_config
from utils.common.string_util import split_long_paragraph_into_chunks

from model.discord.discord_message import DiscordMessage
from model.discord.ipo_message import IPOMessage

from constant.query.oracle_query import OracleQuery
from constant.discord.discord_channel import DiscordChannel

IPO_LIST_LINK = 'https://www.nasdaq.com/market-activity/ipos'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}

# Refresh Time
REFRESH_INTERVAL = get_config('IPO_INFO_SCRAPER', 'REFRESH_INTERVAL')
STACKTRACE_CHUNK_SIZE = get_config('SYS_PARAM', 'STACKTRACE_CHUNK_SIZE')
SCANNER_FATAL_ERROR_REFRESH_INTERVAL = get_config('SYS_PARAM', 'SCANNER_FATAL_ERROR_REFRESH_INTERVAL')
SELENIUM_DRIVER_PATH = get_config('SYS_PARAM', 'SELENIUM_DRIVER_PATH')

CHROME_SERVICE = webdriver.ChromeService(executable_path=SELENIUM_DRIVER_PATH)
OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument('--ignore-certificate-errors')
OPTIONS.add_argument('--ignore-ssl-errors')

session = requests.Session()
logger = Logger()

# Chatbot
ipo_info_scraper_chatbot = DiscordChatBotClient()

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
    
    if len(result) >= 1:
        if len(result) == 1:
            if result[0][5] != ipo_datetime:
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
    
    if len(response) > 0:
        if not hasattr(response[0], 'embeds'):
            ipo_info_scraper_chatbot.send_message(message=DiscordMessage(content=f'Failed to send message to {DiscordChannel.IPO_LIST.value}, {response[0]}'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
            raise Exception(f'Failed to send message to {DiscordChannel.IPO_LIST.value}, {response[0]}')
        
        jump_url = response[0].jump_url
        readout_msg = message.readout_msg
        ticker = message.ticker
        notification_message = DiscordMessage(ticker=ticker, jump_url=jump_url, content=readout_msg)
        ipo_info_scraper_chatbot.send_message(message=notification_message, channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
    else:  
        ipo_info_scraper_chatbot.send_message(message=DiscordMessage(content=f'No response returned from message {message} to {DiscordChannel.IPO_LIST.value}'), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
def scrap():
    # Chatbot Token
    IPO_INFO_SCRAPER_CHATBOT_TOKEN = os.environ['DISCORD_IPO_INFO_SCRAPER_CHATBOT_TOKEN']
    CHATBOT_THREAD_NAME = 'ipo_info_scraper_chatbot_thread'

    ipo_info_scraper_chatbot.run_chatbot(CHATBOT_THREAD_NAME, IPO_INFO_SCRAPER_CHATBOT_TOKEN)
    ipo_info_scraper_chatbot.send_message_by_list_with_response([DiscordMessage(content='Starts scanner')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
    
    start_time = time.time()
    
    ipo_info_dict = {}
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=OPTIONS) 
    # https://stackoverflow.com/questions/47694922/how-to-set-the-timeout-of-driver-get-for-python-selenium-3-8-0
    driver.set_page_load_timeout(10)
        
    try:
        driver.get(IPO_LIST_LINK)
        logger.log_debug_msg(f'Load IPO list page time: {time.time() - start_time} seconds')
    except Exception as e:
        logger.log_error_msg(f'IPO list page loading error: {e}')
    
    try:
        #https://stackoverflow.com/questions/77105233/how-do-i-get-an-element-from-shadow-dom-in-selenium-in-python
        WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')

        nsdq_tables = driver.find_elements(By.CSS_SELECTOR, "nsdq-table-sort") 
        for nsdq_table in nsdq_tables:
            table_type = (nsdq_table.find_elements(By.XPATH, "../../div/h2")[0]
                                    .get_attribute("textContent")
                                    .replace('\n', '')
                                    .strip())

            if table_type != 'Upcoming':
                continue

            shadow_context = driver.execute_script('return arguments[0].shadowRoot', nsdq_table)
            table_row = shadow_context.find_elements(By.CSS_SELECTOR, 'div[part="nsdq-table"] div[part="table-row"]')
            for row in table_row:
                cells = row.find_elements(By.CSS_SELECTOR, 'div[part="table-cell"]')

                details_dict = {}
                for cell_idx, cell in enumerate(cells):
                    if cell_idx == 0:
                        ticker = (cell.get_attribute("textContent")
                                      .replace('\n', '')
                                      .strip())
                        details_link = cell.find_element(By.TAG_NAME, "a").get_attribute('href')
                        
                        details_dict['ticker'] = ticker
                        details_dict['details_link'] = details_link
                    else:
                        try:
                            expected_ipo_date = datetime.strptime(cell.get_attribute('textContent').replace('\n', '').strip(), "%m/%d/%Y")
                            details_dict['ipo_date'] = expected_ipo_date
                        except ValueError:
                            continue
                
                is_added = check_if_ipo_added(ticker, expected_ipo_date)
                
                if not is_added:
                    ipo_info_dict[ticker] = details_dict
        
        driver.close()
        driver.quit()
        
        details_scrape_driver = webdriver.Chrome(service=CHROME_SERVICE, options=OPTIONS) 
        
        for ticker, details_dict in ipo_info_dict.items():
            details_link = details_dict['details_link']
            details_scrape_driver.set_page_load_timeout(20)

            try:
                details_scrape_driver.execute_script("window.open('');")
                details_scrape_driver.switch_to.window(details_scrape_driver.window_handles[-1]) 
                details_scrape_driver.get(details_link)
            except Exception as ex:
                logger.log_error_msg(f'IPO details of {ticker} loading error: {ex}')
            finally:
                WebDriverWait(details_scrape_driver, 20).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                # Company Address, 
                details_element_list = details_scrape_driver.find_elements(By.CSS_SELECTOR, 'div[class="insert-data"] tr')
                for details_element in details_element_list:
                    field_name = (details_element.find_element(By.TAG_NAME, 'th')
                                                 .get_attribute("textContent")
                                                 .replace('\n', '')
                                                 .strip())
                    value = (details_element.find_element(By.TAG_NAME, 'td')
                                            .get_attribute("textContent")
                                            .replace('\n', '')
                                            .strip())
                    details_dict[field_name] = value
                description = (details_scrape_driver.find_element(By.CSS_SELECTOR, 'div[class="description-data"] span')
                                                    .get_attribute("textContent")
                                                    .replace('\n', '')
                                                    .strip())
                details_dict['description'] = description
           
        details_scrape_driver.close()
        details_scrape_driver.quit()
        
        insert_ipo_record_list = []
        for ticker, details_dict in ipo_info_dict.items():
            price_str = details_dict.get('Share Price').replace(",", "").replace("$", "")
            
            if '-' in price_str:
                number_str = price_str.split('-')[-1] 
            else: 
                number_str = price_str
            
            price = float(number_str)
            
            number_of_share = int(details_dict.get('Shares Offered'))
            offering_amount = int(details_dict.get('Offer amount').replace(",", "").replace("$", ""))
            description = details_dict.get('description')
            company_name = details_dict.get('Company Name')
            address = details_dict.get('Company Address')
            website = 'https://' + details_dict.get('Company Website')
            ipo_date = details_dict.get('ipo_date')
            no_of_employee = int(re.match(r'(\d+)', details_dict.get('Employees')).group(1)) if re.match(r'(\d+)', ipo_info_dict.get(ticker).get('Employees')) else None

            title = f'{company_name} Initial Public Offering on ({ipo_date.strftime("%Y-%m-%d")})'
            
            if len(description) > 1024:
                description = description[:1020] + '...'
            
            message = IPOMessage(title=title,
                                 readout_msg=title,
                                 offering_price=price,
                                 offering_shares=number_of_share,
                                 offering_amount=offering_amount,
                                 description=description,
                                 company_website=website,
                                 ticker=ticker)
            insert_ipo_record_list.append([ticker,
                                           company_name,
                                           price,
                                           number_of_share,
                                           offering_amount,
                                           ipo_date,
                                           website,
                                           address,
                                           no_of_employee])
            send_message(message)
            add_ipo_record(insert_ipo_record_list)
    except Exception as exception:
        ipo_info_scraper_chatbot.send_message_by_list_with_response([DiscordMessage(content='Fatal error')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)   
        stacktrace = traceback.format_exc()
        if len(stacktrace) > STACKTRACE_CHUNK_SIZE:
            send_chunk_list = split_long_paragraph_into_chunks(stacktrace, STACKTRACE_CHUNK_SIZE)
            for send_chunk in send_chunk_list:
                ipo_info_scraper_chatbot.send_message_by_list_with_response([DiscordMessage(content=send_chunk)], channel_type=DiscordChannel.CHATBOT_ERROR_LOG)    
        else:
            ipo_info_scraper_chatbot.send_message(DiscordMessage(content=stacktrace), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
        
        logger.log_error_msg(f'Scanner fatal error, {exception}', with_std_out=True)
        logger.log_debug_msg(f'Retry scanning due to fatal error after: {SCANNER_FATAL_ERROR_REFRESH_INTERVAL} seconds', with_std_out=True)
        time.sleep(SCANNER_FATAL_ERROR_REFRESH_INTERVAL)

def run():
    while True: 
        scrap()
        time.sleep(REFRESH_INTERVAL)

if __name__ == '__main__':
    scrap()