from pandas.core.frame import DataFrame
import pandas as pd
import numpy as np

from constant.indicator.runtime_indicator import RuntimeIndicator

idx = pd.IndexSlice

def derive_idx_df(src_df: DataFrame, numeric_idx: bool = True) -> DataFrame:
    if numeric_idx:
        idx_np = src_df.reset_index(drop=True).reset_index().iloc[:, [0]].values
    else:
        idx_np = src_df.reset_index().iloc[:, [0]].values
    
    return pd.DataFrame(np.repeat(idx_np, len(src_df.columns), axis=1), 
                        columns=src_df.columns).rename(columns={src_df.columns.get_level_values(1).values[0]: RuntimeIndicator.INDEX.value})

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