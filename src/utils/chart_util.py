import datetime
import math
import pandas as pd
import numpy as np
import mplfinance as mpf
from matplotlib import dates, ticker

from utils.math_util import round_to_nth_digit
from utils.logger import Logger

from constant.candle.bar_size import BarSize
from constant.indicator.indicator import Indicator

idx = pd.IndexSlice
logger = Logger()

CHART_ROOT_DIR = 'C:/Users/John/Downloads/Trade History/Scanner/Charts'

CHART_WIDTH_PIXEL = 1920
CHART_HEIGHT_PIXEL = 1080

MINUTE_CANDLE_DISPLAY_FORMAT = '%H:%M'
DAILY_CANDLE_DISPLAY_FORMAT = '%Y-%m-%d'

PRICE_GRID_DIVISION = 5
VOLUME_GRID_DIVISION = 3

GRID_STYLE = '--'
LABEL_LINE_COLOUR = "white"
LABEL_TEXT_COLOUR = "white"
LABEL_STYLE_DICT = {
    "xtick.color": LABEL_LINE_COLOUR, 
    "ytick.color": LABEL_LINE_COLOUR, 
    "xtick.labelcolor": LABEL_TEXT_COLOUR, 
    "ytick.labelcolor": LABEL_TEXT_COLOUR,
    'ytick.labelsize': 18,
    'xtick.labelsize': 18,
    "axes.spines.top": False, 
    "axes.spines.right": False
}

DEFAULT_CHART_STYLE = mpf.make_mpf_style(base_mpf_style='tradingview', 
                                         facecolor='#474647', 
                                         figcolor='#474647', 
                                         gridstyle=GRID_STYLE, 
                                         rc=LABEL_STYLE_DICT)

COMMON_CHART_SETTING = dict(type='candle',
                            volume=True,
                            #show_nontrading=True,
                            width_adjuster_version='v0',
                            figsize =(CHART_WIDTH_PIXEL/100, CHART_HEIGHT_PIXEL/100),
                            style=DEFAULT_CHART_STYLE,
                            scale_width_adjustment=dict(volume=1.5,candle=1.5),
                            tight_layout=True,
                            panel_ratios=(1,0.3),
                            figratio=(16,9),
                            figscale=0.8)
                            #panel_ratios=(1,0.3),
                            #scale_width_adjustment=dict(volume=0.7,candle=0.7),
                            #scale_padding=dict(left=0.1, right=0.6, top=0.6, bottom=0.6),)
                            
DESCRIPTION_X_AXIS_OFFSET = -0.35
INDICATOR_Y_AXIS_TICK_OFFSET_FACTOR = 2
MIN_PRICE_RANGE_Y_AXIS_TICK_OFFSET_FACTOR = 2

def get_candlestick_chart(pattern: str, bar_size: BarSize, main_df: pd.DataFrame, scatter_symbol_df = None, scatter_colour_df = None, description_df = None, description_offset: float = DESCRIPTION_X_AXIS_OFFSET) -> str:
    chart_setting = COMMON_CHART_SETTING
    ticker_name = main_df.columns.get_level_values(0).values[0]
    
    ticker_level_dropped_df = main_df.copy()
    ticker_level_dropped_df.columns = ticker_level_dropped_df.columns.droplevel(0)
    
    price_min_range = ticker_level_dropped_df[Indicator.LOW.value].min()
    price_max_range = ticker_level_dropped_df[Indicator.HIGH.value].max()
    volume_max_range = ticker_level_dropped_df[Indicator.VOLUME.value].max()
    
    price_range_difference = price_max_range - price_min_range
    round_precision = 3 if price_min_range < 1 else 2
    price_y_axis_grid_tick = round((price_range_difference / PRICE_GRID_DIVISION), round_precision)
    
    if price_y_axis_grid_tick < 1:
        offseted_price_min_range_list = [price_min_range - (i * price_y_axis_grid_tick) for i in range(1, MIN_PRICE_RANGE_Y_AXIS_TICK_OFFSET_FACTOR + 1)]
        offseted_price_min_range_list.sort()
    else:
        offseted_price_min_range_list = [price_y_axis_grid_tick]
        
    no_of_price_tick =  math.ceil(price_range_difference / price_y_axis_grid_tick)
    price_tick_list = [price_min_range + (i * price_y_axis_grid_tick) for i in range(no_of_price_tick + 2)]
    full_price_tick_list = offseted_price_min_range_list
    for price_tick in price_tick_list:
        full_price_tick_list.append(price_tick)
    
    logger.log_debug_msg(f'{ticker_name} price axis grid tick: {price_y_axis_grid_tick}')
    logger.log_debug_msg(f'{ticker_name} no of price tick: {no_of_price_tick}')
    logger.log_debug_msg(f'{ticker_name} price tick list: {price_tick_list}')
    
    round_volume_max_range = round_to_nth_digit(volume_max_range, 1)
    logger.log_debug_msg(f'{ticker_name} volume range: 0 - {round_volume_max_range}')

    volume_y_axis_grid_tick = round_to_nth_digit((round_volume_max_range / VOLUME_GRID_DIVISION), 2)
    volume_tick_list = [0 + (i * volume_y_axis_grid_tick) for i in range(VOLUME_GRID_DIVISION + 1)]
    logger.log_debug_msg(f'{ticker_name} volume axis grid tick: {volume_y_axis_grid_tick}')
    logger.log_debug_msg(f'{ticker_name} volume tick list: {volume_tick_list}')
    
    ticker_level_dropped_scatter_symbol_df = scatter_symbol_df.copy()
    ticker_level_dropped_scatter_symbol_df.columns = ticker_level_dropped_scatter_symbol_df.columns.droplevel(0)
    
    dt_str_list = []
    x_axis_unit = None
    description_dict_list = []
    
    if bar_size == BarSize.ONE_DAY:
        chart_setting.update(dict(datetime_format = DAILY_CANDLE_DISPLAY_FORMAT, 
                                  show_nontrading=False,
                                  scale_padding=dict(left=0.5, right=3, bottom=1.5, top=2),
))
    elif bar_size == BarSize.ONE_MINUTE:
        dt_str_list = [dt.strftime((MINUTE_CANDLE_DISPLAY_FORMAT)) for dt in main_df.index.tolist()]
        x_axis_unit = dates.MinuteLocator(interval=1)
        chart_setting.update(dict(datetime_format = MINUTE_CANDLE_DISPLAY_FORMAT, 
                                  show_nontrading=True,
                                  scale_padding=dict(left=0.5, right=3, bottom=1, top=2),
))
    
    row_counter = 0
    if description_df is not None:
        for idx_datetime, description_series in description_df.iterrows():
            if bar_size == BarSize.ONE_DAY:
                x_axis_val = row_counter + description_offset
            elif bar_size == BarSize.ONE_MINUTE:
                x_axis_val = dates.date2num((idx_datetime + pd.Timedelta(minutes=description_offset)).to_pydatetime())
            description_dict_list.append(
                dict(x_axis = x_axis_val, 
                     y_axis = main_df.loc[idx_datetime, (ticker_name, Indicator.HIGH.value)],
                     description = description_series[0]
            ))
            
            row_counter = row_counter + 1
    
    if scatter_symbol_df is not None and scatter_colour_df is not None:
        indicator_plot = mpf.make_addplot(np.repeat(full_price_tick_list[1], len(main_df)),
                                           type='scatter', 
                                           markersize=800, 
                                           marker=scatter_symbol_df.values.flatten(), 
                                           color=scatter_colour_df.values.flatten())
        
        indicator_chart_setting = dict(addplot=indicator_plot)
        chart_setting.update(indicator_chart_setting)
        
    chart, axis_list = mpf.plot(ticker_level_dropped_df,
                                returnfig=True,
                                **chart_setting)
    
    if description_dict_list:
        for description in description_dict_list:
            axis_list[0].text(description['x_axis'], 
                              description['y_axis'], 
                              description['description'], 
                              fontsize=14, 
                              fontweight='bold', 
                              color='white')
    
    axis_list[0].set_ylabel('')
    axis_list[2].set_ylabel('')

    if full_price_tick_list:
        axis_list[0].set_yticks(full_price_tick_list)
        axis_list[0].set_yticklabels([str(tick) for tick in full_price_tick_list])
        axis_list[0].yaxis.set_major_locator(ticker.MultipleLocator(price_y_axis_grid_tick))
        axis_list[0].yaxis.set_major_formatter(ticker.FormatStrFormatter(f'%.{round_precision}f'))
        
    if volume_tick_list:
        axis_list[2].set_yticks(volume_tick_list)
        axis_list[2].set_yticklabels([str(tick) for tick in volume_tick_list])
        axis_list[2].yaxis.set_major_locator(ticker.MultipleLocator(volume_y_axis_grid_tick))

    if x_axis_unit and dt_str_list:
        new_x_ticks = [dates.date2num(dt) for dt in main_df.index.tolist()]
        axis_list[3].set_xticks(new_x_ticks)
        axis_list[3].set_xticklabels(dt_str_list)
        axis_list[3].xaxis.set_major_locator(x_axis_unit)
    
    current_datetime_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    output_dir =  f"{CHART_ROOT_DIR}/{pattern}_{ticker_name}_{bar_size.value}_{current_datetime_str}.png"
    chart.savefig(output_dir)
    
    return output_dir
        