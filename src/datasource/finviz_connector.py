from bs4 import BeautifulSoup

from utils.web_scraping_util import scrap_web_data
from utils.filter_util import get_finviz_scanner_filter

from model.finviz.snapshot import Snapshot

from constant.scanner.scanner_target import ScannerTarget

FINVIZ_LINK = 'https://finviz.com/screener.ashx'

class FinvizConnector:
    def __init__(self) -> None:
        self.__yesterday_top_gainer_result_list = []
            
    def __extract_screener_table_column_data(self, response):
        contents = response.text
        soup = BeautifulSoup(contents, 'lxml')
        top_gainer_list = soup.select('table.screener_table tr.styled-row')
    
        for top_gainer in top_gainer_list:
            column_list = top_gainer.find_all('td')
            ticker = column_list[1].text
            change_pct = float(column_list[-2].text[:-1])

            snapshot = Snapshot(ticker, change_pct)
            self.__yesterday_top_gainer_result_list.append(snapshot)
        
    def get_yesterday_top_gainer(self) -> list:
        screener_filter = get_finviz_scanner_filter(ScannerTarget.TOP_GAINER)
        scrap_web_data(FINVIZ_LINK, screener_filter, self.__extract_screener_table_column_data)
        return self.__yesterday_top_gainer_result_list