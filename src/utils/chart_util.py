import datetime
import math
import pandas as pd
import mplfinance as mpf
from matplotlib import dates, ticker

from utils.math_util import round_to_nth_digit

from constant.candle.bar_size import BarSize
from constant.indicator.indicator import Indicator

idx = pd.IndexSlice

CHART_ROOT_DIR = 'C:/Users/John/Downloads/Trade History/Scanner/Charts'

CHART_WIDTH_PIXEL = 1920
CHART_HEIGHT_PIXEL = 1080

MINUTE_CANDLE_DISPLAY_FORMAT = '%H:%M'
DAILY_CANDLE_DISPLAY_FORMAT = '%Y-%m-%d'

SCALE_DIVISION = 5

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
                            show_nontrading=True,
                            width_adjuster_version='v0',
                            figsize =(CHART_WIDTH_PIXEL/100, CHART_HEIGHT_PIXEL/100),
                            style=DEFAULT_CHART_STYLE,
                            scale_padding=dict(left=0.5, right=2.5, bottom=0.5),
                            scale_width_adjustment=dict(volume=1.5,candle=1.5),
                            tight_layout=True,
                            panel_ratios=(1,0.3))
                            #figratio=(16,9),
                            #figscale=0.85,
                            #panel_ratios=(1,0.3),
                            #scale_width_adjustment=dict(volume=0.7,candle=0.7),
                            #scale_padding=dict(left=0.1, right=0.6, top=0.6, bottom=0.6),)
                            
DESCRIPTION_X_AXIS_OFFSET = -0.15
INDICATOR_Y_AXIS_TICK_OFFSET_FACTOR = 2
MIN_PRICE_RANGE_Y_AXIS_TICK_OFFSET_FACTOR = 3

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
    price_y_axis_grid_tick = round((price_range_difference / SCALE_DIVISION), round_precision)
    
    offseted_price_min_range = (price_min_range - (MIN_PRICE_RANGE_Y_AXIS_TICK_OFFSET_FACTOR * price_y_axis_grid_tick))
    offseted_price_max_range = price_min_range + (price_y_axis_grid_tick * math.ceil(((price_max_range - price_min_range) / price_y_axis_grid_tick)))
    
    volume_y_axis_grid_tick = round_to_nth_digit((volume_max_range / 3), 2)
    
    ticker_level_dropped_scatter_symbol_df = scatter_symbol_df.copy()
    ticker_level_dropped_scatter_symbol_df.columns = ticker_level_dropped_scatter_symbol_df.columns.droplevel(0)
    
    if bar_size == BarSize.ONE_DAY:
        x_axis_unit = dates.DayLocator()
        chart_setting.update(dict(datetime_format = DAILY_CANDLE_DISPLAY_FORMAT))
    elif bar_size == BarSize.ONE_MINUTE:
        x_axis_unit = dates.MinuteLocator(interval=1)
        chart_setting.update(dict(datetime_format = '%H:%M'))
    
    description_dict_list = []
    
    if description_df is not None:
        for idx_datetime, description_series in description_df.iterrows():
            description_dict_list.append(
                dict(x_axis = dates.date2num((idx_datetime + pd.Timedelta(minutes=description_offset)).to_pydatetime()), 
                     y_axis = main_df.loc[idx_datetime, (ticker_name, Indicator.HIGH.value)],
                     description = description_series[0]
            ))
    
    if scatter_symbol_df is not None and scatter_colour_df is not None:
        indicator_plot = mpf.make_addplot(ticker_level_dropped_df[Indicator.LOW.value] - (2 * price_y_axis_grid_tick), 
                                           type='scatter', 
                                           markersize=450, 
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
                              fontsize=16, 
                              fontweight='bold', 
                              color='white')
    
    axis_list[0].yaxis.set_major_locator(ticker.MultipleLocator(price_y_axis_grid_tick))
    axis_list[0].set_ylabel('')
    axis_list[0].set_ylim(offseted_price_min_range, offseted_price_max_range)
    axis_list[2].yaxis.set_major_locator(ticker.MultipleLocator(volume_y_axis_grid_tick))
    axis_list[2].set_ylabel('')
    
    new_x_ticks = [dates.date2num(dt) for dt in main_df.index.tolist()]
    axis_list[3].set_xticks(new_x_ticks)
    axis_list[3].set_xticklabels(dt_str_list)
    axis_list[3].xaxis.set_major_locator(x_axis_unit)
    
    
    # j = ['** ']
    # j2 = [offseted_price_min_range - price_y_axis_grid_tick]
    # test = [offseted_price_min_range + (i * price_y_axis_grid_tick) for i in range(math.ceil(((price_max_range - price_min_range) / price_y_axis_grid_tick)))]
    # j2.extend(test)
    # test2 = [str(t) for t in test]
    # j.extend(test2)
    
    # axis_list[0].set_yticks(j2)
    # axis_list[0].set_yticklabels(j)
    
    # axis_list[2].set_yticks([0, 1200, 2400, 6000])
    # axis_list[2].set_yticklabels(['0', '1200', '2400', '     *'])
    # axis_list[2].yaxis.set_major_locator(ticker.MultipleLocator(volume_y_axis_grid_tick))
    

    current_datetime_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    output_dir =  f"{CHART_ROOT_DIR}/{pattern}_{ticker_name}_{bar_size.value}_{current_datetime_str}.png"
    chart.savefig(output_dir)
    
    return output_dir
        