import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime
from queue import Queue
from serpapi import GoogleSearch

from utils.datetime_util import get_current_us_datetime
from utils.http_util import send_async_request
from utils.logger import Logger

ACCOUNT_QUERY_ENDPOINT = 'https://serpapi.com/account.json'
MAX_NO_OF_RESULTS_PER_PAGE = 200
MAX_SEARCH_DURATION_IN_YEAR = 2

#https://developers.google.com/custom-search/docs/xml_results#PhraseSearchqt
FILTER_RESULT_TITLE_REGEX = r'\b(closing|completes)\b'
OFFERING_NEWS_KEYWORDS = 'intitle:"offering"'
TRUNCATE_COMPANY_NAME_SUFFIX_REGEX = r'\b(ltd\.?|plc\.?|adr|inc\.?|corp\.?|llc\.?|class|co\b|ab)-?.?\b'
PUNCTUATION_REGEX = r"""!"#$%&'()*+,./:;<=>?@[\]^_`{|}~"""

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
        result_dict = {}
        
        for api_key in API_KEY_LIST: 
            payload_list.append(dict(api_key=api_key))
                
        try:
            get_api_limit_start_time = time.time()
            account_response_list = send_async_request(method='GET', 
                                                       endpoint=ACCOUNT_QUERY_ENDPOINT, 
                                                       payload_list=payload_list,
                                                       chunk_size=5)
            logger.log_debug_msg(f'Get account API limit response time: {time.time() - get_api_limit_start_time} seconds')
        except Exception as e:
            logger.log_error_msg(f'An error occurred while getting account api limit: {e}')
        else:
            for account_response in account_response_list:
                api_key = account_response.get("api_key")
                remaining_limits = account_response.get("total_searches_left")
                result_dict[api_key] = remaining_limits
                
            for api_key in API_KEY_LIST:
                API_KEYS_TO_LIMIT_DICT[api_key] = result_dict[api_key]
    
    #https://serpapi.com/integrations/python#pagination-using-iterator      
    def search_offering_news(self, contract_list: list) -> dict:
        if not contract_list:
            return {}
        
        ticker_to_datetime_to_news_dict = {}
        self.update_account_info()
        
        end_date = get_current_us_datetime()
        start_date = end_date.replace(year=end_date.year - MAX_SEARCH_DURATION_IN_YEAR)
        
        end_date_str = f'{end_date.month}/{end_date.day}/{end_date.year}' 
        start_date_str = f'{start_date.month}/{start_date.day}/{start_date.year}' 
        advanced_search_query = f'cd_min:{start_date_str},cd_max:{end_date_str}'
            
        search = GoogleSearch({
            'google_domain': 'google.com',
            'hl': 'en',
            'device': 'desktop',    
            'tbs': advanced_search_query,
            'num': MAX_NO_OF_RESULTS_PER_PAGE,
            'filter': 0
        })
            
        fetch_size = len(contract_list)    
        async_api_key = None
        
        for api_key, limit in API_KEYS_TO_LIMIT_DICT.items():
            if limit >= fetch_size:
                async_api_key = api_key
                break
            
        if async_api_key:
           self.async_search(async_api_key, contract_list, ticker_to_datetime_to_news_dict, search)
        else:
           logger.log_debug_msg(f'Account with enough API limit is not avaliable for async search size of {fetch_size}')
           self.sync_search(contract_list, ticker_to_datetime_to_news_dict, search)
        
        logger.log_debug_msg('search completed')
        return ticker_to_datetime_to_news_dict            
    
    def sync_search(self, contract_list: list, ticker_to_datetime_to_news_dict: dict, search: GoogleSearch):
        logger.log_debug_msg('Search google result synchronously')
        search_start_time = time.time()
        
        no_of_completed_response = 0
        chunk_start_idx = 0
        for api_key, limit in API_KEYS_TO_LIMIT_DICT.items():
            if limit > 0:
                contract_chunk_list = contract_list[chunk_start_idx:chunk_start_idx + limit]
                for contract in contract_chunk_list:
                    filtered_result = {}
                    search_ticker_offering_news_start_time = time.time()
                    company_name = (re.sub(TRUNCATE_COMPANY_NAME_SUFFIX_REGEX, '', 
                                           contract.get("company_name"), 
                                           flags=re.IGNORECASE)
                                      .translate(str.maketrans('', '', PUNCTUATION_REGEX))
                                      .strip())
                    ticker = contract.get("symbol")
                    query = f'(intext:"{ticker}" AND intext:"{company_name}") AND (intitle:"{company_name}" AND {OFFERING_NEWS_KEYWORDS})'
                    logger.log_debug_msg(f'Google search query for {ticker}, {company_name}: {query}')

                    #https://ahrefs.com/blog/google-advanced-search-operators/
                    search.params_dict['q'] = query
                    search.params_dict['api_key'] = api_key
                    results = search.get_json()                

                    organic_results = results.get('organic_results')
                    logger.log_debug_msg(msg=organic_results)

                    if not organic_results:
                        ticker_to_datetime_to_news_dict[ticker] = {}
                        continue
                        
                    for _, search_result in enumerate(organic_results):
                        position = search_result.get('position')
                        title = search_result.get('title')
                        snippet = search_result.get('snippet')
                        link = search_result.get('link')
                        source = search_result.get('source')

                        parsed_date = None

                        if 'date' in search_result:
                            date = search_result.get('date')

                            try:
                                parsed_date = datetime.strptime(date, "%b %d, %Y")
                            except ValueError:
                                continue
                                
                            filter_title_pattern = re.compile(FILTER_RESULT_TITLE_REGEX, re.IGNORECASE)
                            is_title_included_filtered_words = filter_title_pattern.search(title)
                            checking_title = title.lower().translate(str.maketrans('', '', PUNCTUATION_REGEX))
                            checking_snippet = snippet.lower()
                            if (parsed_date 
                                    and parsed_date not in filtered_result 
                                    and company_name.lower() in checking_title 
                                    and ('offering' in checking_title or 'offering' in checking_snippet)
                                    and not is_title_included_filtered_words):
                                result_obj = {
                                    'position': position,
                                    'title': title,
                                    'snippet': snippet,
                                    'link': link,
                                    'date': parsed_date,
                                    'source': source
                                }

                                filtered_result[parsed_date] = result_obj

                    ordered_filter_dict = OrderedDict(sorted(filtered_result.items(), key=lambda t: t[0]))
                    ticker_to_datetime_to_news_dict[ticker] = ordered_filter_dict
                    logger.log_debug_msg(f'Search time for {ticker}, {company_name}\'s offering news: {time.time() - search_ticker_offering_news_start_time}s')         

                chunk_start_idx = chunk_start_idx + limit
        
        logger.log_debug_msg(f'Total sync search time for {[contract.get("symbol") for contract in contract_list]}, {time.time() - search_start_time}s', with_std_out=True)
                    
    def async_search(self, api_key: str, contract_list: list, ticker_to_datetime_to_news_dict: dict, search: GoogleSearch):
        logger.log_debug_msg('Search google result asynchronously')
        search_start_time = time.time()
        search_queue = Queue()
        
        for contract in contract_list:
            company_name = (re.sub(TRUNCATE_COMPANY_NAME_SUFFIX_REGEX, '', 
                                           contract.get("company_name"), 
                                           flags=re.IGNORECASE)
                                .translate(str.maketrans('', '', PUNCTUATION_REGEX))
                                .strip())
            ticker = contract.get("symbol")
            query = f'(intext:"{ticker}" AND intext:"{company_name}") AND (intitle:"{company_name}" AND {OFFERING_NEWS_KEYWORDS})'
            logger.log_debug_msg(f'Google search query for {ticker}, original company name: {contract.get("company_name")}, adjusted company name: {company_name}: {query}')

            search.params_dict['q'] = query
            search.params_dict['api_key'] = api_key
            search.params_dict["async"] = True
            
            result = search.get_dict()
            result['symbol'] = ticker
            result['company_name'] = company_name
            
            if "error" in result:
                logger.log_error_msg(f'Async search failed to fetch result for {ticker}, error: {result["error"]}')
                continue
            
            search_queue.put(result)
            
        while not search_queue.empty():
            result = search_queue.get()
            filtered_result = {}
            
            queue_ticker = result.get('symbol')
            queue_company_name = result.get('company_name')
            
            if result.get('search_metadata'):
                organic_results = result.get('organic_results')
                logger.log_debug_msg(msg=organic_results)

                if not organic_results:
                    ticker_to_datetime_to_news_dict[queue_ticker] = {}
                    continue
                
                for search_result in organic_results:
                    position = search_result.get('position')
                    title = search_result.get('title')
                    snippet = search_result.get('snippet')
                    link = search_result.get('link')
                    source = search_result.get('source')  
                    
                    parsed_date = None

                    if 'date' in search_result:
                        date = search_result.get('date')

                        try:
                            parsed_date = datetime.strptime(date, "%b %d, %Y")
                        except ValueError:
                            continue
                                
                    filter_title_pattern = re.compile(FILTER_RESULT_TITLE_REGEX, re.IGNORECASE)
                    is_title_included_filtered_words = filter_title_pattern.search(title)
                    checking_title = title.lower().translate(str.maketrans('', '', PUNCTUATION_REGEX))
                    checking_snippet = snippet.lower().replace('.',' ')
                    print()
                    
                    if (parsed_date 
                            and parsed_date not in filtered_result 
                            and queue_company_name.lower() in checking_title 
                            and ('offering' in checking_title or 'offering' in checking_snippet)
                            and not is_title_included_filtered_words):
                        result_obj = {
                            'position': position,
                            'title': title,
                            'snippet': snippet,
                            'link': link,
                            'date': parsed_date,
                            'source': source
                        }

                        filtered_result[parsed_date] = result_obj
                
                ordered_filter_dict = OrderedDict(sorted(filtered_result.items(), key=lambda t: t[0]))
                ticker_to_datetime_to_news_dict[queue_ticker] = ordered_filter_dict
            else:
                # requeue search_queue
                logger.log_debug_msg(f"{queue_ticker} requeue search")
                search_queue.put(result)
                time.sleep(0.5)
            
        logger.log_debug_msg(f'Total async search time for {[contract.get("symbol") for contract in contract_list]}, {time.time() - search_start_time}s', with_std_out=True)
