from datetime import timedelta
import datetime
import time
import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame

from utils.logger import Logger

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour
from constant.indicator.scatter_colour import ScatterColour
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.matplot_finance import MatplotFinance

logger = Logger()
idx = pd.IndexSlice

def derive_idx_df(src_df: DataFrame, numeric_idx: bool = True) -> DataFrame:
    if numeric_idx:
        idx_np = src_df.reset_index(drop=True).reset_index().iloc[:, [0]].values
    else:
        idx_np = src_df.reset_index().iloc[:, [0]].values
    
    return pd.DataFrame(np.repeat(idx_np, len(src_df.columns), axis=1), 
                        columns=src_df.columns).rename(columns={src_df.columns.get_level_values(1).values[0]: RuntimeIndicator.INDEX.value})

def append_customised_indicator(src_df: pd.DataFrame) -> pd.DataFrame:
    construct_dataframe_start_time = time.time()
    open_df = src_df.loc[:, idx[:, Indicator.OPEN.value]].rename(columns={Indicator.OPEN.value: RuntimeIndicator.COMPARE.value})
    close_df = src_df.loc[:, idx[:, Indicator.CLOSE.value]].rename(columns={Indicator.CLOSE.value: RuntimeIndicator.COMPARE.value})
    vol_df = src_df.loc[:, idx[:, Indicator.VOLUME.value]]

    close_pct_df = close_df.pct_change().mul(100).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CLOSE_CHANGE.value})
    
    flat_candle_df = (open_df == close_df).replace({True: CandleColour.GREY.value, False: np.nan})
    green_candle_df = (close_df > open_df).replace({True: CandleColour.GREEN.value, False: np.nan})
    red_candle_df = (close_df < open_df).replace({True: CandleColour.RED.value, False: np.nan})
    colour_df = ((flat_candle_df.fillna(green_candle_df))
                                .fillna(red_candle_df)
                                .rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CANDLE_COLOUR.value}))

    vol_cumsum_df = vol_df.cumsum().rename(columns={Indicator.VOLUME.value: CustomisedIndicator.TOTAL_VOLUME.value})

    close_above_open_boolean_df = (close_df > open_df)
    close_above_open_upper_body_df = close_df.where(close_above_open_boolean_df.values)
    open_above_close_upper_body_df = open_df.where((~close_above_open_boolean_df).values)
    candle_upper_body_df = close_above_open_upper_body_df.fillna(open_above_close_upper_body_df)

    close_above_open_lower_body_df = open_df.where(close_above_open_boolean_df.values)
    open_above_close_lower_body_df = close_df.where((~close_above_open_boolean_df).values)
    candle_lower_body_df = close_above_open_lower_body_df.fillna(open_above_close_lower_body_df)

    shifted_upper_body_df = candle_upper_body_df.shift(periods=1)
    shifted_lower_body_df = candle_lower_body_df.shift(periods=1)
    gap_up_boolean_df = (candle_lower_body_df > shifted_upper_body_df)
    gap_down_boolean_df = (candle_upper_body_df < shifted_lower_body_df)
    no_gap_boolean_df = ((~gap_up_boolean_df) & (~gap_down_boolean_df))
    
    gap_up_pct_df = (((candle_lower_body_df.sub(shifted_upper_body_df.values))
                                           .div(shifted_upper_body_df.values))
                                           .mul(100)
                                           .where(gap_up_boolean_df.values)).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.GAP_PCT_CHANGE.value})
    gap_down_pct_df = (((candle_upper_body_df.sub(shifted_lower_body_df.values))
                                             .div(candle_upper_body_df.values))
                                             .mul(100)
                                             .where(gap_down_boolean_df.values)).rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.GAP_PCT_CHANGE.value})
    gap_pct_df = ((gap_up_pct_df.fillna(gap_down_pct_df)
                                .where(~no_gap_boolean_df.values)))
    complete_df = pd.concat([src_df, 
                            close_pct_df,
                            gap_pct_df,
                            vol_cumsum_df,
                            colour_df,
                            candle_lower_body_df.rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CANDLE_LOWER_BODY.value}),
                            candle_upper_body_df.rename(columns={RuntimeIndicator.COMPARE.value: CustomisedIndicator.CANDLE_UPPER_BODY.value})], axis=1)

    logger.log_debug_msg(f'Construct customised statistics dataframe time: {time.time() - construct_dataframe_start_time}')
    return complete_df

def concat_daily_df_and_minute_df(daily_df: DataFrame, 
                                  minute_df: DataFrame, 
                                  hit_scanner_datetime: datetime.datetime, 
                                  is_hit_scanner_datetime_start_range: bool = True,
                                  gap_btw_daily_and_minute: int = 1):
    concat_daily_candle_df = daily_df.copy()
    daily_date_to_fake_minute_datetime_x_axis_dict = {}
    dt_index_list = []
    
    for position, dt in enumerate(daily_df.index):
        offset = timedelta(minutes=(len(daily_df) - position) + gap_btw_daily_and_minute)
        daily_date_to_fake_minute_datetime_x_axis_dict.update({hit_scanner_datetime - offset: dt})
        dt_index_list.append(hit_scanner_datetime - offset)
    
    concat_daily_candle_df.index = pd.DatetimeIndex(dt_index_list)
    
    if is_hit_scanner_datetime_start_range:
        candle_chart_data_df = minute_df.loc[hit_scanner_datetime:, :] 
    else:
        candle_chart_data_df = minute_df.loc[:hit_scanner_datetime, :] 
    
    candle_chart_data_df = pd.concat([concat_daily_candle_df, candle_chart_data_df], axis=0)
    
    return candle_chart_data_df, daily_date_to_fake_minute_datetime_x_axis_dict

def replace_daily_df_latest_day_with_minute(daily_df: DataFrame, minute_df: DataFrame):
    daily_df_column_list = list(daily_df.columns.get_level_values(1).unique())
    
    concat_minute_df = minute_df.loc[:, idx[:, daily_df_column_list]].copy()
    yesterday_close_df = daily_df.loc[[daily_df.index[-1]], idx[:, Indicator.CLOSE.value]]
    yesterday_candle_upper_body_df = daily_df.loc[[daily_df.index[-1]], idx[:, CustomisedIndicator.CANDLE_UPPER_BODY.value]]
    minute_close_df = minute_df.loc[:, idx[:, Indicator.CLOSE.value]]
    minute_candle_lower_body_df = minute_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_BODY.value]]
    close_pct_df = (minute_close_df.sub(yesterday_close_df.values)
                                   .div(yesterday_close_df.values)
                                   .mul(100)).rename(columns={Indicator.CLOSE.value: CustomisedIndicator.CLOSE_CHANGE.value})
    gap_up_pct_df = (minute_candle_lower_body_df.sub(yesterday_candle_upper_body_df.values)
                                                .div(yesterday_candle_upper_body_df.values)
                                                .mul(100)).rename(columns={CustomisedIndicator.CANDLE_LOWER_BODY.value: CustomisedIndicator.GAP_PCT_CHANGE.value})    
    concat_minute_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE.value]] = close_pct_df
    concat_minute_df.loc[:, idx[:, CustomisedIndicator.GAP_PCT_CHANGE.value]] = gap_up_pct_df
    concat_minute_df.index = concat_minute_df.index.floor('D')
    
    concat_daily_df = daily_df
    
    if daily_df.index.isin(concat_minute_df.index).any():
        concat_daily_df = daily_df[~daily_df.index.isin(concat_minute_df.index)]
        
    return pd.concat([concat_daily_df,
                      concat_minute_df], axis=0)

def get_scatter_symbol_and_colour_df(src_df: DataFrame, occurrence_idx_list: list, scatter_symbol: ScatterSymbol, scatter_colour: ScatterColour):
    symbol_df = src_df.copy()
    symbol_df.loc[:, :] = np.full(src_df.shape, 'none')
    symbol_df.loc[occurrence_idx_list, :] = scatter_symbol.value
    
    colour_df = src_df.copy()
    colour_df.loc[:, :] = np.full(src_df.shape, 'none')
    colour_df.loc[occurrence_idx_list, :] = scatter_colour.value
    
    return symbol_df, colour_df

def get_candle_comments_df(src_df: DataFrame, indicator_list: list = [CustomisedIndicator.CLOSE_CHANGE, Indicator.VOLUME]):
    ticker_name = src_df.columns.get_level_values(0)[0]
    
    max_no_of_indicator_character = 0
    for indicator in indicator_list:
        no_of_character = len(indicator.value)
        if no_of_character >= max_no_of_indicator_character:
            max_no_of_indicator_character = no_of_character
    
    indicator_description_np = np.full((src_df.shape[0], 1), '')
    for indicator in indicator_list:
        no_of_whitespace_padding = max_no_of_indicator_character - len(indicator.value)
        whitespace = ''.join(np.repeat(' ', no_of_whitespace_padding))
        
        src_indicator_df = src_df.loc[:, idx[[ticker_name], indicator.value]]
        
        copied_array = src_indicator_df.values.copy()
        string_array = copied_array.astype(str)
        
        if indicator == CustomisedIndicator.CLOSE_CHANGE or indicator == CustomisedIndicator.GAP_PCT_CHANGE:
            src_df_value_np = np.around(src_indicator_df.values.astype(float), 2)
            
            for pos, arry in enumerate(src_df_value_np):
                if pd.isna(arry[0]):
                    string_array[pos][0] = ''
                else:
                    string_array[pos][0] = f'{indicator.value + whitespace}: {arry[0]}%\n'
        elif indicator == Indicator.VOLUME:
            for pos, arry in enumerate(src_indicator_df.values):
                if pd.isna(arry[0]):
                    string_array[pos][0] = f'{indicator.value + whitespace}: NA\n'
                else:
                    string_array[pos][0] = f'{indicator.value + whitespace}: {"{:,}".format(int(arry[0]))}\n'
        elif indicator == Indicator.CLOSE:
            src_df_value_np = np.around(src_indicator_df.values.astype(float), 3)

            for pos, arry in enumerate(src_df_value_np):
                string_array[pos][0] = f'{indicator.value + whitespace}: ${arry[0]}\n'
        
        indicator_description_np = np.char.add(indicator_description_np, string_array) 
        
    indicator_description_df = pd.DataFrame(indicator_description_np, 
                                            columns=pd.MultiIndex.from_product([[ticker_name], [MatplotFinance.DESCRIPTION.value]]),
                                            index=src_df.index)
    
    return indicator_description_df

def get_ticker_to_occurrence_idx_list(occurrence_df: DataFrame, occurrence_limit: int = None) -> dict:
    result_dict = {}
    ticker_list = occurrence_df.columns.get_level_values(0).unique().tolist()
    idx_df = derive_idx_df(occurrence_df)

    occurrence_cumsum_df = occurrence_df.cumsum().where(occurrence_df.values) 
    
    if occurrence_limit:
        truncated_occurrence_cumsum_df = occurrence_cumsum_df.where((occurrence_cumsum_df <= occurrence_limit).values)
        normalised_idx_df = idx_df.where(truncated_occurrence_cumsum_df.notnull().values)
        normalised_cumsum_idx_np = np.sort(normalised_idx_df.values.T)[:, :occurrence_limit] 
    else:
        normalised_idx_df = idx_df.where(occurrence_cumsum_df.notnull().values)
        normalised_cumsum_idx_np = np.sort(normalised_idx_df.values.T)
    
    for index, ticker in enumerate(ticker_list):
        cumsum_idx_list = normalised_cumsum_idx_np[index] if not np.isnan(normalised_cumsum_idx_np[index]).all() else []
        datetime_idx_list = [occurrence_df.index[int(cumsum_idx)] if not np.isnan(cumsum_idx) else None for cumsum_idx in cumsum_idx_list]
        result_dict[ticker] = datetime_idx_list
        
    return result_dict
    
def get_sorted_value_without_duplicate_df(src_df: DataFrame) -> DataFrame:
    sorted_np = np.sort(src_df.values, axis=0)
    _, indices = np.unique(sorted_np.flatten(), return_inverse=True)
    mask = np.ones(sorted_np.size, dtype=bool)
    mask[indices] = False
    mask = mask.reshape(sorted_np.shape)
    sorted_np[mask] = -1
    sorted_df = pd.DataFrame(np.sort(sorted_np, axis=0), 
                             columns=src_df.columns).replace(-1, np.nan)   
    
    return sorted_df

def get_idx_df_by_value_df(val_src_df: DataFrame, idx_src_df: DataFrame, numeric_idx: bool = True) -> DataFrame:
    result_df = val_src_df.copy()
    
    if numeric_idx:
        reference_df = idx_src_df.reset_index(drop=True)
    else:
        reference_df = idx_src_df
    
    for column in result_df.columns:
        result_df[column] = result_df[column].apply(lambda x: reference_df.index[reference_df[column] == x][0] if pd.notnull(x) else np.nan)
    
    return result_df