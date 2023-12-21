import time
import pandas as pd
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from constant.candle.candle_colour import CandleColour
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.indicator import Indicator
from constant.indicator.runtime_indicator import RuntimeIndicator

from utils.dataframe_util import derive_idx_df
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class UnusualVolumeRampUp(PatternAnalyser):
    MIN_MARUBOZU_RATIO = 65
    MIN_CLOSE_PCT = 4.2
    MIN_VOLUME = 3000
    NOTIFY_PERIOD = 2
        
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        logger.log_debug_msg('Unusual ramp up scan')
        start_time = time.time()

        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
        close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        previous_close_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE]]
        previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]]
        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR]].rename(columns={CustomisedIndicator.CANDLE_COLOUR: RuntimeIndicator.COMPARE})
        marubozu_ratio_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MARUBOZU_RATIO]].rename(columns={CustomisedIndicator.MARUBOZU_RATIO: RuntimeIndicator.COMPARE})
        volume_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]].rename(columns={Indicator.VOLUME: RuntimeIndicator.COMPARE})
        total_volume_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.TOTAL_VOLUME]]
        vol_20_ma_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MA_20_VOLUME]].rename(columns={CustomisedIndicator.MA_20_VOLUME: RuntimeIndicator.COMPARE})
        vol_50_ma_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MA_50_VOLUME]].rename(columns={CustomisedIndicator.MA_50_VOLUME: RuntimeIndicator.COMPARE})
        
        green_candle_df = (candle_colour_df == CandleColour.GREEN)
        marubozu_boolean_df = (marubozu_ratio_df >= self.MIN_MARUBOZU_RATIO)
        candle_close_pct_boolean_df = (close_pct_df >= self.MIN_CLOSE_PCT)
        ramp_up_boolean_df = (green_candle_df) & (marubozu_boolean_df) & (candle_close_pct_boolean_df)
        above_vol_20_ma_boolean_df = (volume_df >= vol_20_ma_df) & (vol_20_ma_df >= self.MIN_VOLUME) & (ramp_up_boolean_df)
        above_vol_50_ma_boolean_df = (volume_df >= vol_50_ma_df) & (vol_50_ma_df >= self.MIN_VOLUME) & (ramp_up_boolean_df)

        above_vol_20_ma_result_boolean_df = above_vol_20_ma_boolean_df.iloc[-self.NOTIFY_PERIOD:]
        above_vol_20_ma_result_series = above_vol_20_ma_result_boolean_df.any()
        above_vol_20_ma_ticker_list = above_vol_20_ma_result_series.index[above_vol_20_ma_result_series].get_level_values(0).tolist()
        
        above_vol_50_ma_result_boolean_df = above_vol_50_ma_boolean_df.iloc[-self.NOTIFY_PERIOD:]
        above_vol_50_ma_result_series = above_vol_50_ma_result_boolean_df.any()
        above_vol_50_ma_ticker_list = above_vol_50_ma_result_series.index[above_vol_50_ma_result_series].get_level_values(0).tolist()

        above_vol_20_ma_ticker_list = [ticker for ticker in above_vol_20_ma_ticker_list if ticker not in above_vol_50_ma_ticker_list]
        
        logger.log_debug_msg(f'Unusual volume analysis time: {time.time() - start_time} seconds')
        
        if len(above_vol_20_ma_ticker_list) > 0 or len(above_vol_50_ma_ticker_list) > 0:
            log_start_time = time.time()
            result_ticker_list = [above_vol_20_ma_ticker_list, above_vol_50_ma_ticker_list]

            for list_idx, ticker_list in enumerate(result_ticker_list):
                if len(ticker_list) > 0:
                    ma_val = '20' if (list_idx == 0) else '50'
                    above_ma_df = above_vol_20_ma_boolean_df if (list_idx == 0) else above_vol_50_ma_boolean_df
                    ma_vol_df = vol_20_ma_df if (list_idx == 0) else vol_50_ma_df
    
                    datetime_idx_df = derive_idx_df(above_ma_df, numeric_idx=False)
                    ramp_up_datetime_idx_df = datetime_idx_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_close_df = close_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_close_pct_df = close_pct_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_previous_close_df = previous_close_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_previous_close_pct_df = previous_close_pct_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_volume_df = volume_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_total_volume_df = total_volume_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    ramp_up_ma_vol_df = ma_vol_df.where(above_ma_df.values).ffill().iloc[[-1]]
    
                    for ticker in ticker_list:
                        display_close = ramp_up_close_df.loc[:, ticker].iat[0, 0]
                        volume = ramp_up_volume_df.loc[:, ticker].iat[0, 0]
                        display_volume = "{:,}".format(volume)
                        display_total_volume = "{:,}".format(ramp_up_total_volume_df.loc[:, ticker].iat[0, 0])
                        display_close_pct = round(ramp_up_close_pct_df.loc[:, ticker].iat[0, 0], 2)
                        display_ma_vol = ramp_up_ma_vol_df.loc[:, ticker].iat[0, 0]
                        display_previous_close = ramp_up_previous_close_df.loc[:, ticker].iat[0, 0]
                        display_previous_close_pct = round(ramp_up_previous_close_pct_df.loc[:, ticker].iat[0, 0], 2)
    
                        ramp_up_datetime = ramp_up_datetime_idx_df.loc[:, ticker].iat[0, 0]
                        ramp_up_hour = pd.to_datetime(ramp_up_datetime).hour
                        ramp_up_minute = pd.to_datetime(ramp_up_datetime).minute
                        display_hour = ('0' + str(ramp_up_hour)) if ramp_up_hour < 10 else ramp_up_hour
                        display_minute = ('0' + str(ramp_up_minute)) if ramp_up_minute < 10 else ramp_up_minute
                        display_time_str = f'{display_hour}:{display_minute}'
                        read_time_str = f'{ramp_up_hour} {ramp_up_minute}' if (ramp_up_minute > 0) else f'{ramp_up_hour} o clock' 
                        read_ticker_str = " ".join(ticker)
    
                        logger.log_debug_msg(f'{ticker} ramp up {display_close_pct}% above {ma_val}MA volume at {display_time_str}, {ma_val}MA volume: {display_ma_vol}, Volume: {display_volume}, Total volume: {display_total_volume}, Volume ratio: {round((float(volume)/ display_ma_vol), 1)}, Close: ${display_close}, Previous close: {display_previous_close}, Previous close change: {display_previous_close_pct}%', with_std_out = True)
                        logger.log_debug_msg(f'{read_ticker_str} ramp up {display_close_pct} percent above {ma_val} M A volume at {read_time_str}, Ratio: {round((float(volume)/ display_ma_vol), 1)}', with_speech = True, with_log_file = False)
                        
            logger.log_debug_msg(f'Read and log unusual ramp up result time: {time.time() - log_start_time} seconds')


