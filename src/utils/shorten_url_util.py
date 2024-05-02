import os
import time

from utils.http_util import send_async_request
from utils.logger import Logger

from constant.endpoint.tinyurl.tinyurl_api_endpoint import TinyUrlApiEndpoint

TINY_URL_TOKEN = os.environ['TINY_URL_TOKEN']
REQUEST_HEADER = {'Authorization': f'Bearer {TINY_URL_TOKEN}', 'Content-Type': 'application/json'}

logger = Logger()

def shorten_url(url_list: list) -> dict:
    shorten_url_payload_list = [dict(url=url, domain=TinyUrlApiEndpoint.SHORTENED_URL_DOMAIN) for url in url_list]
    
    try:
        start_time = time.time()
        shorten_url_response_list = send_async_request(method='POST', 
                                                      endpoint=TinyUrlApiEndpoint.HOSTNAME + TinyUrlApiEndpoint.SHORTEN_URL, 
                                                      headers=REQUEST_HEADER,
                                                      payload_list=shorten_url_payload_list, 
                                                      chunk_size=20)
        logger.log_debug_msg(f'Request shorten url time: {time.time() - start_time}')
    except Exception as shorten_url_request_exception:
        logger.log_error_msg(f'An error occurred while requesting shorten url, Cause: {shorten_url_request_exception}')
        raise shorten_url_request_exception
    else:
        incompleted_response_list = []
        
        url_to_shortened_url_dict = {}
        for idx, shorten_url_response in enumerate(shorten_url_response_list):
            data = shorten_url_response.get('data')
            errors = shorten_url_response.get('errors')
            
            if not data or errors:
                original_url = url_list[idx]
                url_to_shortened_url_dict[original_url] = original_url
                
                incompleted_response_list.append(original_url)
                logger.log_debug_msg(f'Not able to fetch shortened url for {original_url}, cause: {errors}')
            else:
                shortened_url = data.get('tiny_url')
                original_url = data.get('url')
                url_to_shortened_url_dict[original_url] = shortened_url
                logger.log_debug_msg(f'original url: {original_url}, shortened url: {shortened_url}', with_std_out=True)
        
        if len(incompleted_response_list):
            logger.log_debug_msg(f'No. of incompleted response: {len(incompleted_response_list)}')
    
        return url_to_shortened_url_dict
                    
