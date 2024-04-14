import datetime
import time
import pandas as pd

from model.discord.scanner_result_message import ScannerResultMessage
from model.financial_data import FinancialData
from model.offering_news import OfferingNews

from pattern.pattern_analyser import PatternAnalyser

from utils.discord_message_record_util import check_if_pattern_analysis_message_sent
from utils.chart_util import get_candlestick_chart
from utils.logger import Logger

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour
from constant.candle.bar_size import BarSize
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.scatter_colour import ScatterColour
from constant.discord.discord_channel import DiscordChannel

idx = pd.IndexSlice
logger = Logger()
PATTERN_NAME = 'YESTERDAY_BULLISH_DAILY_CANDLE'

class YesterdayBullishDailyCandle(PatternAnalyser):
    MIN_YESTERDAY_CLOSE_CHANGE_PCT = 30
    
    def __init__(self, hit_scanner_date: datetime.date, daily_df: pd.DataFrame, ticker_to_contract_info_dict: dict, discord_client, sqlite_connector):
        super().__init__(discord_client, sqlite_connector)
        self.__sqlite_connector = sqlite_connector
        self.__hit_scanner_date = hit_scanner_date
        self.__historical_data_df = daily_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict
        self.__filtered_ticker_list = []
        
    def __members(self):
        return (self.__ticker_to_offerings_news_dict,
                self.__ticker_to_financial_data_dict,
                self.__filtered_ticker_list)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, YesterdayBullishDailyCandle):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    @property
    def ticker_to_offerings_news_dict(self):
        return self.__ticker_to_offerings_news_dict
    
    @ticker_to_offerings_news_dict.setter
    def ticker_to_offerings_news_dict(self, ticker_to_offerings_news_dict):
        self.__ticker_to_offerings_news_dict = ticker_to_offerings_news_dict
        
    @property
    def ticker_to_financial_data_dict(self):
        return self.__ticker_to_financial_data_dict
    
    @ticker_to_financial_data_dict.setter
    def ticker_to_financial_data_dict(self, ticker_to_financial_data_dict):
        self.__ticker_to_financial_data_dict = ticker_to_financial_data_dict

    @property
    def filtered_ticker_list(self):
        return self.__filtered_ticker_list
    
    @filtered_ticker_list.setter
    def filtered_ticker_list(self, filtered_ticker_list):
        self.__filtered_ticker_list = filtered_ticker_list

    def get_and_update_filtered_result(self) -> list:
        close_pct_df = self.__historical_data_df.iloc[[-1]].loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]].rename(columns={CustomisedIndicator.CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
        green_candle_df = self.__historical_data_df.iloc[[-1]].loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]].rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        min_close_pct_boolean_df = (close_pct_df >= self.MIN_YESTERDAY_CLOSE_CHANGE_PCT)
        green_candle_boolean_df = (green_candle_df == CandleColour.GREEN.value)
        yesterday_bullish_daily_candle_boolean_df = (min_close_pct_boolean_df) & (green_candle_boolean_df)
        
        with pd.option_context('display.max_rows', None,
                                       'display.max_columns', None,
                                    'display.precision', 3):
            logger.log_debug_msg(f'Bullish Daily Candle Boolean Dataframe: {yesterday_bullish_daily_candle_boolean_df}')
        
        yesterday_bullish_daily_candle_result_series = yesterday_bullish_daily_candle_boolean_df.any()   
        result_list = yesterday_bullish_daily_candle_result_series.index[yesterday_bullish_daily_candle_result_series].get_level_values(0).tolist()
        
        ticker_to_close_pct_dict = {}
        for ticker in result_list:
            close_pct = close_pct_df.loc[:, (ticker, 'Compare')].values[0]
            ticker_to_close_pct_dict[ticker] = close_pct
        
        sorted_dict = {k: v for k, v in sorted(ticker_to_close_pct_dict.items(), key=lambda item: item[1])}
        filtered_ticker_list = []
        for ticker in sorted_dict:
            filtered_ticker_list.append(ticker)
        
        analysis_ticker_list = []
        for ticker in filtered_ticker_list:
            is_yesterday_bullish_candle_analysis_msg_sent = check_if_pattern_analysis_message_sent(connector=self.__sqlite_connector, 
                                                                                                   ticker=ticker, 
                                                                                                   hit_scanner_datetime=self.__hit_scanner_date, 
                                                                                                   scan_pattern=PATTERN_NAME, 
                                                                                                   bar_size=BarSize.ONE_DAY.value)
            
            if not is_yesterday_bullish_candle_analysis_msg_sent:
                analysis_ticker_list.append(ticker)
        
        self.__filtered_ticker_list = analysis_ticker_list
        
        return self.__filtered_ticker_list
    
    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Bullish candle scan')
        
        for ticker in self.__filtered_ticker_list:
            contract_info = self.__ticker_to_contract_info_dict.get(ticker)
            date_to_news_dict = self.__ticker_to_offerings_news_dict.get(ticker)
            financial_data_dict = self.__ticker_to_financial_data_dict.get(ticker)
            financial_data = FinancialData(symbol=ticker, financial_data_dict=financial_data_dict)
            offering_news = OfferingNews(symbol=ticker, date_to_news_dict=date_to_news_dict)
                
            close = self.__historical_data_df.loc[self.__hit_scanner_date.strftime('%Y-%m-%d'), (ticker, Indicator.CLOSE.value)]
            close_pct = self.__historical_data_df.loc[self.__hit_scanner_date.strftime('%Y-%m-%d'), (ticker, CustomisedIndicator.CLOSE_CHANGE.value)]
            volume = self.__historical_data_df.loc[self.__hit_scanner_date.strftime('%Y-%m-%d'), (ticker, Indicator.VOLUME.value)]
                    
            daily_chart_dir = get_candlestick_chart(candle_data_df=self.__historical_data_df,
                                                    ticker=ticker, pattern=PATTERN_NAME, bar_size=BarSize.ONE_DAY,
                                                    hit_scanner_datetime=self.__hit_scanner_date,
                                                    scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.CYAN,
                                                    candle_comment_list=[CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE])
              
            message = ScannerResultMessage(title=f'{ticker}\'s yesterday\'s bullish daily candle, up {round(close_pct, 2)}% ({self.__hit_scanner_date})',
                                           readout_msg=f'{" ".join(ticker)} yesterday\'s bullish daily candle, up {round(close_pct, 2)}%',
                                           close=close,
                                           yesterday_close=close,
                                           total_volume=volume,
                                           contract_info=contract_info,
                                           financial_data=financial_data,
                                           offering_news=offering_news,
                                           daily_chart_dir=daily_chart_dir, 
                                           ticker=ticker,
                                           hit_scanner_datetime=self.__hit_scanner_date,
                                           pattern=PATTERN_NAME,
                                           bar_size=BarSize.ONE_DAY.value)
            message_list.append(message)
        
        if message_list:
            send_msg_start_time = time.time()
            self.send_notification(message_list, DiscordChannel.YESTERDAY_BULLISH_DAILY_CANDLE, False)
            logger.log_debug_msg(f'{PATTERN_NAME} send message time: {time.time() - send_msg_start_time} seconds')
        