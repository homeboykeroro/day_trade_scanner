import time
import requests

from utils.logger import Logger

session = requests.Session()
logger = Logger()

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}

def scrap_web_data(website_link: str, payload: dict, callback):
    try:
        scrap_star_time = time.time()
        response = session.get(website_link, params=payload, headers=HEADERS)
        logger.log_debug_msg(f'Scrap {website_link} response time: {time.time() - scrap_star_time} seconds')
        # Raises a HTTPError if the response status is 4xx, 5xx
        response.raise_for_status() 
    except Exception as request_exception:
        logger.log_error_msg(f'An error occurred while scarping data: {request_exception}')
    else:
        callback(response)