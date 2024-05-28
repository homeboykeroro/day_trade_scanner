import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime
from queue import Queue
from serpapi import GoogleSearch

from module.discord_chatbot_client import DiscordChatBotClient

from model.discord.discord_message import DiscordMessage

from utils.shorten_url_util import shorten_url
from utils.datetime_util import get_current_us_datetime
from utils.http_util import send_async_request
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

ACCOUNT_QUERY_ENDPOINT = 'https://serpapi.com/account.json'
MAX_NO_OF_RESULTS_PER_PAGE = 200
MAX_SEARCH_DURATION_IN_YEAR = 2

#https://developers.google.com/custom-search/docs/xml_results#PhraseSearchqt
FILTER_RESULT_TITLE_REGEX = r'\b(closing|completes)\b'
OFFERING_NEWS_KEYWORDS = 'intitle:"offering"'
TRUNCATE_COMPANY_NAME_SUFFIX_REGEX = r'\b(ltd\.?|plc\.?|adr|inc\.?|corp\.?|llc\.?|class|co\b|ab|gr|soluti)-?.?\b'
EXTRACT_DATE_STR_REGEX = r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b"
PUNCTUATION_REGEX = r"""!"#$%&'()*+,./:;<=>?@[\]^_`{|}~"""

SERP_API_KEY_LIST = os.environ['SERP_API_KEYS']
API_KEY_LIST = []
API_KEYS_TO_LIMIT_DICT = {}

MAX_WAITING_TIME = 10

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
    def search_offering_news(self, contract_list: list, discord_client: DiscordChatBotClient) -> dict:
        if not contract_list:
            return {}
        
        ticker_to_datetime_to_news_dict = {}
        self.update_account_info()
        
        end_date = get_current_us_datetime()
        start_date = end_date.replace(year=end_date.year - MAX_SEARCH_DURATION_IN_YEAR)
        
        end_date_str = f'{end_date.month}/{end_date.day}/{end_date.year}' 
        start_date_str = f'{start_date.month}/{start_date.day}/{start_date.year}' 
        advanced_search_query = f'cd_min:{start_date_str},cd_max:{end_date_str}'
        logger.log_debug_msg(f'Google search date range: {start_date_str} - {end_date_str}')    
        
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
           self.async_search(async_api_key, contract_list, ticker_to_datetime_to_news_dict, search, discord_client)
        else:
           logger.log_debug_msg(f'Account with enough API limit is not avaliable for async search size of {fetch_size}')
           self.sync_search(contract_list, ticker_to_datetime_to_news_dict, search, discord_client)
        
        logger.log_debug_msg('search completed')
        return ticker_to_datetime_to_news_dict            
    
    def sync_search(self, contract_list: list, ticker_to_datetime_to_news_dict: dict, search: GoogleSearch, discord_client: DiscordChatBotClient):
        logger.log_debug_msg('Search google result synchronously')
        search_start_time = time.time()
        shorten_url_list = []
        
        ticker_list = []
        company_name_list = []
        search_query_list = []
        
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

                    ticker_list.append(ticker)
                    company_name_list.append(company_name)
                    search_query_list.append(query)
                    
                    organic_results = results.get('organic_results')
                    logger.log_debug_msg(msg=organic_results)

                    if not organic_results:
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
                        else:
                            match = re.search(EXTRACT_DATE_STR_REGEX, snippet)
                            if match:
                                snippet_date_str = match.group()

                                try:
                                    parsed_date = datetime.strptime(snippet_date_str, "%B %d, %Y")
                                except ValueError:
                                    continue
                                
                            filter_title_pattern = re.compile(FILTER_RESULT_TITLE_REGEX, re.IGNORECASE)
                            is_title_included_filtered_words = filter_title_pattern.search(title)
                            checking_title = title.lower().translate(str.maketrans('', '', PUNCTUATION_REGEX))
                            checking_snippet = snippet.lower().replace('.',' ')
                            
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
                                
                                shorten_url_list.append('link')
                                filtered_result[parsed_date] = result_obj

                    ordered_filter_dict = OrderedDict(sorted(filtered_result.items(), key=lambda t: t[0]))
                    ticker_to_datetime_to_news_dict[ticker] = ordered_filter_dict
                    logger.log_debug_msg(f'Search time for {ticker}, {company_name}\'s offering news: {time.time() - search_ticker_offering_news_start_time}s')         

                chunk_start_idx = chunk_start_idx + limit
        
        original_url_to_shortened_url_dict = shorten_url(shorten_url_list)
        for ticker, datetime_to_news_dict in ticker_to_datetime_to_news_dict.items():
            for _, news_dict in datetime_to_news_dict.items():
                original_url = news_dict.get('link')
                shortened_url = original_url_to_shortened_url_dict.get(original_url) if original_url_to_shortened_url_dict.get(original_url) else original_url
                
                news_dict['shortened_url'] = shortened_url
        
        for contract in contract_list:
            if contract.get('symbol') not in ticker_list:
                logger.log_debug_msg(f'Not enough API limit to fetch {contract.get("symbol")} offering news')
        
        log_msg = f'Original company name list: {[contract.get("company_name") for contract in contract_list]}\n'
        log_msg += f'Adjusted company name list: {company_name_list}\n'
        log_msg += f'Search query list (async): {search_query_list}\n'
            
        discord_client.send_message(DiscordMessage(content=log_msg), DiscordChannel.YESTERDAY_TOP_GAINER_SCANNER_LIST)
        
        logger.log_debug_msg(f'Total sync search time for {[contract.get("symbol") for contract in contract_list]}, {time.time() - search_start_time}s', with_std_out=True)
                    
    def async_search(self, api_key: str, contract_list: list, ticker_to_datetime_to_news_dict: dict, search: GoogleSearch, discord_client: DiscordChatBotClient):
        logger.log_debug_msg('Search google result asynchronously')
        search_start_time = time.time()
        search_queue = Queue()
        shorten_url_list = []
        
        ticker_list = []
        company_name_list = []
        search_query_list = []
        
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
            
            if result is None:
              logger.log_debug_msg(f"data is empty for: {ticker}")
              continue
          
            ticker_list.append(ticker)
            company_name_list.append(company_name)
            search_query_list.append(query)
            result['symbol'] = ticker
            result['company_name'] = company_name
            
            serp_api_account_info_log = ''
            serp_api_account_info_log += 'SERP API remaining limit: ['
            for api_key, limit in API_KEYS_TO_LIMIT_DICT.items():
                serp_api_account_info_log += f'{api_key}: {limit}' + ',\n'
            serp_api_account_info_log += ']\n\n'
            discord_client.send_message(DiscordMessage(content=serp_api_account_info_log), DiscordChannel.SERP_API_ACCOUNT_INFO_LOG)
            
            yesterday_bullish_daily_candle_log = ''
            yesterday_bullish_daily_candle_log += 'Yesterday bullish daily candle ticker list: ['
            for contract_dict in contract_list:
                yesterday_bullish_daily_candle_log += contract_dict.get("symbol") + ',\n'
            yesterday_bullish_daily_candle_log += ']\n\n'
            
            yesterday_bullish_daily_candle_log = ''
            yesterday_bullish_daily_candle_log += 'Original company name list: ['
            for contract in contract_list:
                yesterday_bullish_daily_candle_log += contract.get("company_name") + ',\n'
            yesterday_bullish_daily_candle_log += ']\n\n'
            
            yesterday_bullish_daily_candle_log += 'Adjusted company name list: ['
            for adjusted_name in company_name_list:
                yesterday_bullish_daily_candle_log += adjusted_name + ',\n'
            yesterday_bullish_daily_candle_log += ']\n\n'
            discord_client.send_message(DiscordMessage(content=yesterday_bullish_daily_candle_log), DiscordChannel.YESTERDAY_TOP_GAINER_SCANNER_LIST)
            
            search_query_log = ''
            search_query_log += 'Search query list (async):\n'
            
            for index, search_query in enumerate(search_query_list):
                search_query_log += f'{index + 1}: {search_query}\n' 
                
            discord_client.send_message(DiscordMessage(content=search_query_log), DiscordChannel.SERP_API_SEARCH_QUERY_LOG)
            
            if "error" in result:
                ticker_to_datetime_to_news_dict[ticker] = 'error'
                logger.log_error_msg(f'Async search failed to fetch result for {ticker}, error: {result["error"]}')
                continue
            
            search_queue.put(result)
        
        logger.log_debug_msg(f'Search result for {company_name_list}', with_std_out=True)
        logger.log_debug_msg(f"Adjusted company name list {[re.sub(TRUNCATE_COMPANY_NAME_SUFFIX_REGEX, '', company_name, flags=re.IGNORECASE).translate(str.maketrans('', '', PUNCTUATION_REGEX)).strip() for company_name in company_name_list]}", with_std_out=True)
        
        while not search_queue.empty():
            result = search_queue.get()
            filtered_result = {}
            
            queue_ticker = result.get('symbol')
            queue_company_name = result.get('company_name')
            
            #https://github.com/serpapi/google-search-results-python/blob/master/tests/test_example.py#L107
            #search_id = result.get('search_metadata').get('id')
            #search_archived = search.get_search_archive(search_id)
            #succeeded = re.search('Cached|Success', search_archived['search_metadata']['status'])
            metadata = result.get('search_metadata')
            status = metadata.get('status')
            succeeded = status == 'Cached' or result.get('search_metadata').get('status') == 'Success' if metadata else None
            logger.log_debug_msg(f'ticker queue status: {status}')
            if succeeded:
                organic_results = result.get('organic_results')
                logger.log_debug_msg(msg=organic_results)

                if not organic_results:
                    continue
                
                if organic_results:
                    discord_client.send_message(DiscordMessage(content=f'{queue_ticker} no of organic result: {len(organic_results)}\n'), DiscordChannel.SERP_API_SEARCH_RESULT_LOG)
                else:
                    discord_client.send_message(DiscordMessage(content=f'{queue_ticker} has no organic result\n'), DiscordChannel.SERP_API_SEARCH_RESULT_LOG)
                    
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
                    else:
                        if snippet:
                            match = re.search(EXTRACT_DATE_STR_REGEX, snippet)
                            
                            if match:
                                snippet_date_str = match.group()

                                try:
                                    parsed_date = datetime.strptime(snippet_date_str, "%B %d, %Y")
                                except ValueError:
                                    continue
                                
                    filter_title_pattern = re.compile(FILTER_RESULT_TITLE_REGEX, re.IGNORECASE)
                    is_title_included_filtered_words = filter_title_pattern.search(title)
                    checking_title = title.lower().translate(str.maketrans('', '', PUNCTUATION_REGEX))
                    #checking_snippet = snippet.lower().replace('.',' ')
                    
                    if (parsed_date 
                            and parsed_date not in filtered_result 
                            and queue_company_name.lower() in checking_title 
                            #and ('offering' in checking_title or 'offering' in checking_snippet)
                            and not is_title_included_filtered_words):
                        result_obj = {
                            'position': position,
                            'title': title,
                            'snippet': snippet,
                            'link': link,
                            'date': parsed_date,
                            'source': source
                        }

                        shorten_url_list.append(link)
                        filtered_result[parsed_date] = result_obj
                
                ordered_filter_dict = OrderedDict(sorted(filtered_result.items(), key=lambda t: t[0]))
                ticker_to_datetime_to_news_dict[queue_ticker] = ordered_filter_dict
            else:
                if time.time() - search_start_time > MAX_WAITING_TIME:
                    break
                # requeue search_queue
                logger.log_debug_msg(f"{queue_ticker} requeue search")
                search_queue.put(result)
                time.sleep(1)
        
        original_url_to_shortened_url_dict = shorten_url(shorten_url_list)
        for ticker, datetime_to_news_dict in ticker_to_datetime_to_news_dict.items():
            if datetime_to_news_dict == 'error':
                continue
            
            for _, news_dict in datetime_to_news_dict.items():
                original_url = news_dict.get('link')
                shortened_url = original_url_to_shortened_url_dict.get(original_url) if original_url_to_shortened_url_dict.get(original_url) else original_url
                
                news_dict['shortened_url'] = shortened_url
        
        for ticker in ticker_to_datetime_to_news_dict:
            if ticker not in ticker_list:
                logger.log_debug_msg(f'No result found for {ticker}')
            
        logger.log_debug_msg(f'Total async search time for {[contract.get("symbol") for contract in contract_list]}, {time.time() - search_start_time}s', with_std_out=True)
