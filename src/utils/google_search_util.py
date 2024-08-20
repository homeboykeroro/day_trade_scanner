import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta
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
OFFERING_NEWS_KEYWORDS = '(intext:"offering" OR intext:"announces pricing")'
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
            
        self.sync_search(contract_list, ticker_to_datetime_to_news_dict, search, discord_client)
        
        logger.log_debug_msg('search completed')
        return ticker_to_datetime_to_news_dict            
    
    def get_datetime_from_str_expression(self, date_str: str) -> datetime:
        day_match = re.search(r'(\d+) day[s]? ago', date_str)
        month_match = re.search(r'(\d+) month[s]? ago', date_str)
        result_date = None
        
        if day_match:
            subtract_day = int(day_match.group(1))
            result_date = datetime.now() - timedelta(days=subtract_day)
            
        if month_match:
            subtract_month = int(month_match.group(1))
            current_datetime = datetime.now()
            actual_month = current_datetime.month
            
            result_date = current_datetime.replace(months=actual_month-subtract_month)
        
        return result_date

    def sync_search(self, contract_list: list, ticker_to_datetime_to_news_dict: dict, search: GoogleSearch, discord_client: DiscordChatBotClient):
        logger.log_debug_msg('Search google result synchronously')
        search_start_time = time.time()
        shorten_url_list = []
        
        completed_ticker_list = []
        
        serp_api_account_info_log = ''
        last_key = list(API_KEYS_TO_LIMIT_DICT)[-1]
        serp_api_account_info_log += 'SERP API remaining limit: \n'  
        for api_key, limit in API_KEYS_TO_LIMIT_DICT.items():
            serp_api_account_info_log += f'   {api_key}: {limit}'
            if api_key != last_key:
                serp_api_account_info_log += ', \n'
        serp_api_account_info_log += '\n\n' 
        discord_client.send_message(DiscordMessage(content=serp_api_account_info_log), DiscordChannel.SERP_API_ACCOUNT_INFO_LOG)
        
        yesterday_bullish_daily_candle_log = 'Yesterday bullish daily candle ticker list: \n'
        last_contract = contract_list[-1]
        for index, contract in enumerate(contract_list):
            yesterday_bullish_daily_candle_log += f'   {index + 1}. ' + contract.get("symbol") 
            
            if last_contract.get("symbol") != contract.get("symbol"):
                yesterday_bullish_daily_candle_log += ', \n'
        yesterday_bullish_daily_candle_log += '\n\n'
        
        yesterday_bullish_daily_candle_log += 'Original company name list: \n'
        for index, contract in enumerate(contract_list):
            yesterday_bullish_daily_candle_log += f'   {index + 1}. ' + contract.get("company_name")
            
            if last_contract.get("symbol") != contract.get("symbol"):
                yesterday_bullish_daily_candle_log += ', \n'
        yesterday_bullish_daily_candle_log += '\n\n'
        
        yesterday_bullish_daily_candle_log += 'Adjusted company name list: \n'
            
        search_query_log = 'Search query list (sync): \n'
            
        chunk_start_idx = 0
        for api_key, limit in API_KEYS_TO_LIMIT_DICT.items():
            if limit > 0 and len(completed_ticker_list) != len(contract_list):
                contract_chunk_list = contract_list[chunk_start_idx:chunk_start_idx + limit]
                for index, contract in enumerate(contract_chunk_list):
                    filtered_result = {}
                    ticker = contract.get("symbol")
                    search_ticker_offering_news_start_time = time.time()
                    
                    company_name = (re.sub(TRUNCATE_COMPANY_NAME_SUFFIX_REGEX, '', 
                                           contract.get("company_name"), 
                                           flags=re.IGNORECASE)
                                      .translate(str.maketrans('', '', PUNCTUATION_REGEX))
                                      .strip())
                    query = f'(intext:"{ticker}" AND intext:"{company_name}") AND {OFFERING_NEWS_KEYWORDS})'

                    #https://ahrefs.com/blog/google-advanced-search-operators/
                    search.params_dict['q'] = query
                    search.params_dict['api_key'] = api_key
                    results = search.get_json()                

                    organic_results = results.get('organic_results')
                    
                    if not organic_results:
                        discord_client.send_message(DiscordMessage(content=f'{ticker} has no organic result\n'), DiscordChannel.SERP_API_SEARCH_RESULT_LOG)
                        continue
                    else:
                        discord_client.send_message(DiscordMessage(content=f'{ticker} no of organic result: {len(organic_results)}\n'), DiscordChannel.SERP_API_SEARCH_RESULT_LOG)
                    
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
                                if date.endswith("ago"):
                                    parsed_date = self.get_datetime_from_str_expression(date)
                                else:
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
                        checking_snippet = snippet.lower().replace('.',' ') if snippet else '' 
                        
                        if (parsed_date 
                                and parsed_date not in filtered_result 
                                and (('offering' in checking_title or 'announces' in checking_title)
                                        or ('offering' in checking_snippet or 'announces' in checking_snippet))
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
                    ticker_to_datetime_to_news_dict[ticker] = ordered_filter_dict
                    logger.log_debug_msg(f'Search time for {ticker}, {company_name}\'s offering news: {time.time() - search_ticker_offering_news_start_time}s')         

                    #logging
                    completed_ticker_list.append(ticker)
                    
                    yesterday_bullish_daily_candle_log += f'   {index + 1}. ' + company_name
                    search_query_log += f'   {index + 1}: {query}'  
                    if index < len(contract_chunk_list) - 1:
                        yesterday_bullish_daily_candle_log += ', \n'
                        search_query_log += ', \n'
                    else:
                        yesterday_bullish_daily_candle_log += '\n\n'
                        search_query_log += '\n\n'
        
                    logger.log_debug_msg(f'Google search query for {ticker}, {company_name}: {query}')
                    logger.log_debug_msg(msg=organic_results)
                    
                chunk_start_idx = chunk_start_idx + limit
            else:
                break
        
        discord_client.send_message(DiscordMessage(content=yesterday_bullish_daily_candle_log), DiscordChannel.YESTERDAY_TOP_GAINER_SCANNER_LIST)
        discord_client.send_message(DiscordMessage(content=search_query_log), DiscordChannel.SERP_API_SEARCH_QUERY_LOG)    
        
        original_url_to_shortened_url_dict = shorten_url(shorten_url_list)
        for ticker, datetime_to_news_dict in ticker_to_datetime_to_news_dict.items():
            for _, news_dict in datetime_to_news_dict.items():
                original_url = news_dict.get('link')
                shortened_url = original_url_to_shortened_url_dict.get(original_url) if original_url_to_shortened_url_dict.get(original_url) else original_url
                
                news_dict['shortened_url'] = shortened_url
        
        for contract in contract_list:
            if contract.get('symbol') not in completed_ticker_list:
                discord_client.send_message(DiscordMessage(content=f'Not enought API limit to retrieve offering news for {contract.get("symbol")}'), DiscordChannel.CHATBOT_ERROR_LOG)
                
        logger.log_debug_msg(f'Total sync search time for {[contract.get("symbol") for contract in contract_list]}, {time.time() - search_start_time}s', with_std_out=True)
