from collections import OrderedDict
from datetime import datetime
import os
import re
import threading
import time
from serpapi import GoogleSearch

from utils.previous_day_top_gainer_util import get_latest_scrape_date
from utils.datetime_util import get_current_us_datetime
from utils.http_util import send_async_request
from utils.logger import Logger

ACCOUNT_QUERY_ENDPOINT = 'https://serpapi.com/account.json'
MAX_NO_OF_RESULTS_PER_PAGE = 200
MAX_SEARCH_DURATION_IN_YEAR = 2

#https://developers.google.com/custom-search/docs/xml_results#PhraseSearchqt
FILTER_RESULT_TITLE_REGEX = r'\b(closing|completes)\b'
OFFERING_NEWS_KEYWORDS = 'intitle:"offering"'
TRUNCATE_COMPANY_NAME_SUFFIX_REGEX =  r'\b(ltd\.?|plc\.?|adr|inc\.?|corp\.?|llc\.?)\b'

SERP_API_KEY_LIST = os.environ['SERP_API_KEYS']
API_KEY_LIST = []
API_KEYS_TO_LIMIT_DICT = {}


logger = Logger()

if SERP_API_KEY_LIST:
    API_KEY_LIST = SERP_API_KEY_LIST.split(';')

class GoogleSearchUtil:
    _instance = None
    _lock = threading.Lock()
    
    #https://medium.com/analytics-vidhya/how-to-create-a-thread-safe-singleton-class-in-python-822e1170a7f6
    def __new__(cls):
        if cls._instance is None: 
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    #https://serpapi.com/account-api
    def update_account_info(self):
        payload_list = []
        
        for api_key in API_KEY_LIST: 
            payload_list.append(dict(api_key=api_key))
                
        try:
            get_api_limit_start_time = time.time()
            account_response_list = send_async_request(method='GET', 
                                                       endpoint=ACCOUNT_QUERY_ENDPOINT, 
                                                       payload_list=payload_list,
                                                       chunk_size=5)
            logger.log_debug_msg(f'Get api limit response time: {time.time() - get_api_limit_start_time} seconds', with_std_out=True)
        except Exception as e:
            logger.log_error_msg(f'An error occurred while getting account api limit: {e}')
        else:
            for account_response in account_response_list:
                api_key = account_response.get("api_key")
                remaining_limits = account_response.get("total_searches_left")
                API_KEYS_TO_LIMIT_DICT[api_key] = remaining_limits
    
    #https://serpapi.com/integrations/python#pagination-using-iterator      
    def search_offering_news(self, contract_list: list, sqlite_connector) -> dict:
        ticker_to_datetime_to_news_dict = {}
        self.update_account_info()
        
        end_date = get_current_us_datetime()
        start_date = end_date.replace(year=end_date.year - MAX_SEARCH_DURATION_IN_YEAR)
        
        request_search_news_contract_list = []
        for contract in contract_list:
            ticker = contract.symbol
            scrape_date = get_latest_scrape_date(sqlite_connector, ticker)
            
            if (not scrape_date 
                or (scrape_date.year < end_date.year 
                        or (scrape_date.year == end_date.year and scrape_date.month < end_date.month))):
                request_search_news_contract_list.append(contract)
        
        if request_search_news_contract_list:
            end_date_str = f'{end_date.month}/{end_date.day}/{end_date.year}' 
            start_date_str = f'{start_date.month}/{start_date.day}/{start_date.year}' 

            filter_date_range_str = f'cd_min:{start_date_str},cd_max:{end_date_str}'
            filtered_result = {}
            
            for contract in request_search_news_contract_list:
                company_name = re.sub(TRUNCATE_COMPANY_NAME_SUFFIX_REGEX, '', contract.company_name, flags=re.IGNORECASE).strip()
                ticker = contract.symbol
                query = f'(intext:"{ticker}" AND intext:"{company_name}") AND (intitle:"{company_name}" AND {OFFERING_NEWS_KEYWORDS})'
                
                search = GoogleSearch({
                    #https://ahrefs.com/blog/google-advanced-search-operators/
                    'q': query, 
                    'google_domain': 'google.com',
                    'hl': 'en',
                    'device': 'desktop',    
                    'tbs': filter_date_range_str,
                    'num': MAX_NO_OF_RESULTS_PER_PAGE,
                    'api_key': API_KEY_LIST[0],
                    'filter': 0
                })

                results = search.get_json()                
                
                organic_results = results.get('organic_results')
                logger.log_debug_msg(msg=organic_results, with_log_file=True)
                
                if not organic_results:
                    continue
                
                for idx, search_result in enumerate(organic_results):
                    position = search_result.get('position')
                    title = search_result.get('title')
                    link = search_result.get('link')
                    source = search_result.get('source')
                    
                    parsed_date = None
                    
                    if 'date' in search_result:
                        date = search_result.get('date')
                        
                        try:
                            parsed_date = datetime.strptime(date, "%b %d, %Y")
                        except ValueError:
                            print(f'error date: {date}')
                            continue
                        
                        filter_title_pattern = re.compile(FILTER_RESULT_TITLE_REGEX, re.IGNORECASE)
                        is_title_included_filtered_words = filter_title_pattern.search(title)
                        
                        if (parsed_date not in filtered_result 
                                and company_name in title and 'offering' in title.lower()
                                and not is_title_included_filtered_words):
                            result_obj = {
                                'position': position,
                                'title': title,
                                'link': link,
                                'date': parsed_date,
                                'source': source
                            }
                            
                            filtered_result[parsed_date] = result_obj
                        else:
                            print(f'result not added, idx: {idx}')
                    else:
                        print(f'invalid date on {position}')

                ordered_filter_dict = OrderedDict(sorted(filtered_result.items(), key=lambda t: t[0]))
                ticker_to_datetime_to_news_dict[ticker] = ordered_filter_dict
        
        return ticker_to_datetime_to_news_dict            
            

        
    