import time
import pandas as pd

from model.discord.scanner_result_message import ScannerResultMessage

from pattern.pattern_analyser import PatternAnalyser

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
    
    def __init__(self, daily_df: pd.DataFrame, ticker_to_contract_info_dict: dict, discord_client, sqlite_connector):
        super().__init__(discord_client, sqlite_connector)
        self.__historical_data_df = daily_df
        self.__ticker_to_contract_info_dict = ticker_to_contract_info_dict

    def analyse(self) -> None:
        message_list = []
        logger.log_debug_msg('Bullish candle scan')
        close_pct_df = self.__historical_data_df.iloc[[-1]].loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]].rename(columns={CustomisedIndicator.CLOSE_CHANGE.value: RuntimeIndicator.COMPARE.value})
        green_candle_df = self.__historical_data_df.iloc[[-1]].loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR.value]].rename(columns={CustomisedIndicator.CANDLE_COLOUR.value: RuntimeIndicator.COMPARE.value})
        
        min_close_pct_boolean_df = (close_pct_df >= self.MIN_YESTERDAY_CLOSE_CHANGE_PCT)
        green_candle_boolean_df = (green_candle_df >= CandleColour.GREEN.value)
        yesterday_bullish_daily_candle_boolean_df = (min_close_pct_boolean_df) & (green_candle_boolean_df)
        
        yesterday_bullish_daily_candle_result_series = yesterday_bullish_daily_candle_boolean_df.any()   
        yesterday_bullish_daily_candle_ticker_list = yesterday_bullish_daily_candle_result_series.index[yesterday_bullish_daily_candle_result_series].get_level_values(0).tolist()
        
        hit_scanner_date = self.__historical_data_df.index.tolist()[-1]

        if len(yesterday_bullish_daily_candle_ticker_list) > 0:
            for ticker in yesterday_bullish_daily_candle_ticker_list:
                is_message_sent = self.check_if_pattern_analysis_message_sent(ticker=ticker, hit_scanner_datetime=hit_scanner_date.date(), pattern=PATTERN_NAME, bar_size=BarSize.ONE_DAY)

                if not is_message_sent:
                    with pd.option_context('display.max_rows', None,
                                           'display.max_columns', None,
                                        'display.precision', 3):
                        logger.log_debug_msg(f'{ticker} Yesterday Bullish Daily Candle Dataframe: {self.__historical_data_df.loc[:, idx[[ticker], :]]}')
                        
                    contract_info = self.__ticker_to_contract_info_dict[ticker]
                    close = self.__historical_data_df.loc[hit_scanner_date, (ticker, Indicator.CLOSE.value)]
                    close_pct = self.__historical_data_df.loc[hit_scanner_date, (ticker, CustomisedIndicator.CLOSE_CHANGE.value)]
                    volume = self.__historical_data_df.loc[hit_scanner_date, (ticker, Indicator.VOLUME.value)]
                    
                    daily_chart_dir = get_candlestick_chart(candle_data_df=self.__historical_data_df,
                                                            ticker=ticker, pattern=PATTERN_NAME, bar_size=BarSize.ONE_DAY,
                                                            hit_scanner_datetime=hit_scanner_date,
                                                            scatter_symbol=ScatterSymbol.POP, scatter_colour=ScatterColour.CYAN,
                                                            candle_comment_list=[CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE])
                        
                    hit_scanner_datetime_display = hit_scanner_date.strftime('%Y-%m-%d')
                                            
                    message = ScannerResultMessage(title=f'{ticker}\'s yesterday\'s bullish daily candle, up {round(close_pct, 2)}% ({hit_scanner_datetime_display})',
                                                   readout_msg=f'{" ".join(ticker)} yesterday\'s bullish daily candle, up {round(close_pct, 2)}%',
                                                   close=close,
                                                   yesterday_close=close,
                                                   volume=volume, total_volume=volume,
                                                   contract_info=contract_info,
                                                   daily_chart_dir=daily_chart_dir, 
                                                   ticker=ticker,
                                                   hit_scanner_datetime=hit_scanner_datetime_display,
                                                   pattern=PATTERN_NAME,
                                                   bar_size=BarSize.ONE_DAY)
                    message_list.append(message)
        
        if message_list:
            send_msg_start_time = time.time()
            self.send_notification(message_list, DiscordChannel.YESTERDAY_BULLISH_DAILY_CANDLE)
            logger.log_debug_msg(f'Bullish candle send message time: {time.time() - send_msg_start_time} seconds')
        