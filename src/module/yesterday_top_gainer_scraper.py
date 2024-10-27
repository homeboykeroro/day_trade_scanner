
import re
import time
import traceback
from bs4 import BeautifulSoup
import requests

from module.discord_chatbot_client import DiscordChatBotClient

from model.discord.discord_message import DiscordMessage

from utils.sql.scraper_record_util import check_if_top_gainer_added, add_top_gainer_record
from utils.common.datetime_util import check_if_us_business_day, get_current_us_datetime
from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

FINVIZ_LINK = 'https://finviz.com/screener.ashx'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}
TOP_GAINER_PAYLOAD = {'s': 'ta_topgainers'}

session = requests.Session()
logger = Logger()

EXIT_WAIT_TIME = 30

def scrap(discord_client: DiscordChatBotClient):
    start_time = time.time() 
    scan_date = get_current_us_datetime()
    is_business_day = check_if_us_business_day(scan_date)

    if not is_business_day:
        discord_client.send_message_by_list_with_response([DiscordMessage(content='No data is retrieved, current datetime is not U S business day')], channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
        logger.log_error_msg(f'No data is fetched, current datetime is not US business day', with_std_out=True)
        return
    
    try:
        scrap_star_time = time.time()
        response = session.get(FINVIZ_LINK, params=TOP_GAINER_PAYLOAD, headers=HEADERS)
        logger.log_debug_msg(f'Scrap {FINVIZ_LINK} response time: {time.time() - scrap_star_time} seconds', with_std_out=True)
        # Raises a HTTPError if the response status is 4xx, 5xx
        response.raise_for_status() 
    except Exception as e:
        logger.log_error_msg(f'An error occurred while scarping data: {e}', with_std_out=True)
    else:
        top_gainer_list = []
        contents = response.text
        soup = BeautifulSoup(contents, 'lxml')
        row_list = soup.select('table.screener_table tr.styled-row')

        try:
            for row in row_list:
                column_list = row.find_all('td')
                ticker = column_list[1].text
                
                if not re.match('^[a-zA-Z]{1,4}$', ticker):
                    logger.log_debug_msg(f'Exclude {ticker} from yesterday top gainer list', with_std_out=True)
                    continue
                
                company = column_list[2].text
                sector = column_list[3].text
                industry = column_list[4].text
                country = column_list[5].text
                market_cap_str = column_list[6].text
                close_price = float(column_list[8].text.replace(',', ''))
                change_pct = float(column_list[9].text.replace('%', ''))
                volume = int(column_list[10].text.replace(',', ''))

                market_cap = 0
                if market_cap_str:
                    multiplier = 1

                    if market_cap_str.endswith('K'):
                        multiplier = 1e4
                    if market_cap_str.endswith('M'):
                        multiplier = 1e6
                    elif market_cap_str.endswith('B'):
                        multiplier = 1e9

                    market_cap_str = market_cap_str[:-1]
                    num = float(market_cap_str.replace(',', '')) if market_cap_str else 0
                    market_cap = int(num * multiplier)

                is_gainer_added = check_if_top_gainer_added(ticker, scan_date)  
                if not is_gainer_added:
                    top_gainer_list.append([ticker, 
                                            company, sector, industry, 
                                            scan_date, 
                                            close_price, 
                                            volume, change_pct, 
                                            market_cap, country])

            if top_gainer_list:
                add_top_gainer_record(top_gainer_list)
                discord_client.send_message(DiscordMessage(content=f'Yesterday top gainer history: {[f"{top_gainer[0]} ({top_gainer[1]}): {top_gainer[7]}%" for top_gainer in top_gainer_list]}'), channel_type=DiscordChannel.YESTERDAY_TOP_GAINER_SCRAPER_HISTORY_LOG)
                discord_client.send_message(DiscordMessage(content='Yesterday top gainer history retrieval succeed'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                logger.log_debug_msg(f'Scraped yesterday top gainer: {[f"{top_gainer[0]} ({top_gainer[1]}): {top_gainer[7]}%" for top_gainer in top_gainer_list]}')
            else:
                discord_client.send_message(DiscordMessage(content='No Yesterday day top gainer history is added'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
                
            logger.log_debug_msg(f'Yesterday top gainers scraping completed, finished in {time.time() - start_time} seconds', with_std_out=True)
        except Exception as e:
            discord_client.send_message_by_list_with_response(DiscordMessage(content='Yesterday day top gainer history retrieval failed'), channel_type=DiscordChannel.TEXT_TO_SPEECH, with_text_to_speech=True)
            discord_client.send_message(DiscordMessage(content=traceback.format_exc()), channel_type=DiscordChannel.CHATBOT_ERROR_LOG)
            logger.log_error_msg(f'Yesterday top gainer history retrieval failed, Error: {e}')
            time.sleep(EXIT_WAIT_TIME)