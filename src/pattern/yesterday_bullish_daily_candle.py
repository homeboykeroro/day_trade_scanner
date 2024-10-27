import datetime
import time
import pandas as pd

from model.discord.scanner_result_message import ScannerResultMessage
from model.financial_data import FinancialData
from model.offering_news import OfferingNews
from model.discord.discord_message import DiscordMessage

from pattern.pattern_analyser import PatternAnalyser

from utils.google_search_util import GoogleSearchUtil
from utils.yfinance_util import get_financial_data
from utils.sql.discord_message_record_util import check_if_pattern_analysis_message_sent
from utils.chart_util import get_candlestick_chart
from utils.common.config_util import get_config
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
google_search_util = GoogleSearchUtil()

SHOW_DISCORD_DEBUG_LOG = get_config('YESTERDAY_TOP_GAINER', 'SHOW_DISCORD_DEBUG_LOG')

class YesterdayBullishDailyCandle(PatternAnalyser):
    
    def __init__(self, hit_scanner_date: datetime.date,  daily_df: pd.DataFrame, 
                       ticker_to_contract_info_dict: dict, 
                       discord_client,
                       min_close_pct, 
                       max_offering_news_size,
                       pattern_name):
        super().__init__(discord_client)
        self.__hit_scanner_date = hit_scanner_date
        self.__daily_df = daily_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict
        
        self.__min_close_pct = min_close_pct
        self.__max_offering_news_size = max_offering_news_size
        self.__pattern_name = pattern_name

    @property
    def filtered_contract_list(self):
        return self.__filtered_contract_list
    
    @filtered_contract_list.setter
    def filtered_contract_list(self, filtered_contract_list):
        self.__filtered_contract_list = filtered_contract_list
    
    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Yesterday top gainer bullish daily candle scan')
        
        close_pct_df = self.__daily_df.iloc[[-1]].loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]].rename(columns={CustomisedIndicator.CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
        green_candle_df = self.__daily_df.iloc[[-1]].loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]].rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        min_close_pct_boolean_df = (close_pct_df >= self.__min_close_pct)
        green_candle_boolean_df = (green_candle_df == CandleColour.GREEN.value)
        yesterday_bullish_daily_candle_boolean_df = (min_close_pct_boolean_df) & (green_candle_boolean_df)
        
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 3):
            logger.log_debug_msg(f'Bullish daily candle boolean dataframe:')
            logger.log_debug_msg(yesterday_bullish_daily_candle_boolean_df)
        
        yesterday_bullish_daily_candle_result_series = yesterday_bullish_daily_candle_boolean_df.any()   
        result_list = yesterday_bullish_daily_candle_result_series.index[yesterday_bullish_daily_candle_result_series].get_level_values(0).tolist()
        
        ticker_to_close_pct_dict = {}
        for ticker in result_list:
            close_pct = close_pct_df.loc[:, (ticker, RuntimeIndicator.COMPARE.value)].values[0]
            ticker_to_close_pct_dict[ticker] = close_pct
        
        sorted_ticker_to_close_pct_dict = {k: v for k, v in sorted(ticker_to_close_pct_dict.items(), key=lambda item: item[1], reverse=True)}
        filtered_contract_list = []
        
        for ticker in sorted_ticker_to_close_pct_dict:
            check_start_time = time.time()
            is_yesterday_bullish_candle_analysis_msg_sent = check_if_pattern_analysis_message_sent(ticker=ticker, 
                                                                                                   hit_scanner_datetime=self.__hit_scanner_date, 
                                                                                                   pattern=self.__pattern_name, 
                                                                                                   bar_size=BarSize.ONE_DAY.value)
            logger.log_debug_msg(f'Check if {ticker} pattern analysis exists in db finish time: {time.time() - check_start_time} seconds')
            
            if not is_yesterday_bullish_candle_analysis_msg_sent:
                contract = self.__ticker_to_contract_info_dict.get(ticker)
                symbol = contract.symbol if hasattr(contract, 'symbol') else None
                con_id = contract.con_id if hasattr(contract, 'con_id') else None
                company_name = contract.company_name if hasattr(contract, 'company_name') else None
                filtered_contract_list.append(dict(symbol=symbol, con_id=con_id, company_name=company_name))
        
        if not filtered_contract_list:
            logger.log_debug_msg('None of yesterday top gainer meeting filter criterion', with_std_out=True)
            return
        
        ticker_to_financial_data_dict = get_financial_data(filtered_contract_list)
        ticker_to_offering_news_dict = google_search_util.search_offering_news(filtered_contract_list, self._discord_client)

        filtered_ticker_list = [contract.get('symbol') for contract in filtered_contract_list]

        for ticker in filtered_ticker_list:
            contract_info = self.__ticker_to_contract_info_dict.get(ticker)
            date_to_news_dict = ticker_to_offering_news_dict.get(ticker)
            financial_data_dict = ticker_to_financial_data_dict.get(ticker)
            financial_data = FinancialData(symbol=ticker, financial_data_dict=financial_data_dict)
            offering_news = OfferingNews(symbol=ticker, date_to_news_dict=date_to_news_dict, max_offering_news_size=self.__max_offering_news_size)
            
            date_to_news_dict_size = len(date_to_news_dict) if date_to_news_dict else 0
            
            if SHOW_DISCORD_DEBUG_LOG:
                self._discord_client.send_message(DiscordMessage(content=f'{ticker} offering news size: {date_to_news_dict_size}'), DiscordChannel.OFFERING_NEWS_LOG)
                
            close = self.__daily_df.loc[self.__hit_scanner_date.strftime('%Y-%m-%d'), (ticker, Indicator.CLOSE.value)]
            close_pct = self.__daily_df.loc[self.__hit_scanner_date.strftime('%Y-%m-%d'), (ticker, CustomisedIndicator.CLOSE_CHANGE.value)]
            volume = self.__daily_df.loc[self.__hit_scanner_date.strftime('%Y-%m-%d'), (ticker, Indicator.VOLUME.value)]
            
            chart_start_time = time.time()
            chart_dir = get_candlestick_chart(candle_data_df=self.__daily_df,
                                              ticker=ticker, pattern=self.__pattern_name, bar_size=BarSize.ONE_DAY,
                                              hit_scanner_datetime=self.__hit_scanner_date,
                                              scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.CYAN,
                                              candle_comment_list=[CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE, Indicator.CLOSE, Indicator.VOLUME])
            logger.log_debug_msg(f'Generate {ticker} chart time, {time.time() - chart_start_time} seconds')
            
            message = ScannerResultMessage(title=f'{ticker}\'s yesterday\'s bullish daily candle, up {round(close_pct, 2)}% ({self.__hit_scanner_date})',
                                           readout_msg=f'{" ".join(ticker)} yesterday\'s bullish daily candle, up {round(close_pct, 2)}%',
                                           close=close,
                                           yesterday_close=close,
                                           total_volume=volume,
                                           contract_info=contract_info,
                                           chart_dir=chart_dir,
                                           financial_data=financial_data,
                                           offering_news=offering_news,
                                           ticker=ticker,
                                           hit_scanner_datetime=self.__hit_scanner_date,
                                           pattern=self.__pattern_name,
                                           bar_size=BarSize.ONE_DAY.value)
            message_list.append(message)
        
        if message_list:
            send_msg_start_time = time.time()
            self.send_notification(message_list, DiscordChannel.YESTERDAY_BULLISH_DAILY_CANDLE, False)
            logger.log_debug_msg(f'{self.__pattern_name} send message time: {time.time() - send_msg_start_time} seconds')
        