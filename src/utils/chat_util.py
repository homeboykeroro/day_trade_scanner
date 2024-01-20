import math
import datetime
import pandas as pd
import mplfinance as mpf
from matplotlib import ticker

from constant.candle.bar_size import BarSize
from constant.indicator.indicator import Indicator

idx = pd.IndexSlice

CHART_ROOT_DIR = 'C:/Users/John/Downloads/Trade History/Scanner/Charts/'

CHART_WIDTH_PIXEL = 1920
CHART_HEIGHT_PIXEL = 1080

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

DEFAULT_CHART_STYLE = mpf.make_mpf_style(base_mpf_style='tradingview', facecolor='#474647', figcolor='#474647', gridstyle=GRID_STYLE, rc=LABEL_STYLE_DICT)

CHART_SETTING = dict(type='candle',
                     volume=True,
                     figsize =(CHART_WIDTH_PIXEL/100, CHART_HEIGHT_PIXEL/100),
                     figratio=(16,9),
                     figscale=0.85,
                     panel_ratios=(1,0.3),
                     scale_width_adjustment=dict(volume=0.85,candle=1.35),
                     show_nontrading=True)

def get_candlestick_chart(pattern: str, bar_size: BarSize, main_df: pd.DataFrame, sub_panel_df, candle_start_range, candle_end_range) -> str:
    ticker_name = main_df.columns.get_level_values(0).values[0]
    candle_df = main_df.loc[candle_start_range:candle_end_range, :]
    
    # ticker_level_dropped_df = main_df.copy()
    # ticker_level_dropped_df.columns = ticker_level_dropped_df.columns.droplevel(0)
    
    min_range = candle_df[Indicator.LOW.value].min()
    max_range = candle_df[Indicator.HIGH.value].max()
    
    grid_scale = math.ceil(math.log10(max_range))
    round_precision = 2 if grid_scale >= 0 else 3
    y_axis_grid_divider = round((max_range - min_range) / SCALE_DIVISION, round_precision)
    
    chart, axis_list = mpf.plot(candle_df,
                                style=DEFAULT_CHART_STYLE,
                                returnfig=True,
                                scale_padding=dict(left=0.1, right=0.6, top=0.6, bottom=0.6),
                                datetime_format = '%H:%M',
                                #ylim=(min_range, max_range),
                                **CHART_SETTING)
    
    axis_list[0].yaxis.set_major_locator(ticker.MultipleLocator(y_axis_grid_divider))
    axis_list[0].set_ylim(min_range, max_range)
    axis_list[2].set_ylim(0, 276000)
    axis_list[3].xaxis.set_major_locator(ticker.MultipleLocator(1))
    
    current_datetime_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    chart.savefig(f"{CHART_ROOT_DIR}/{pattern}_{ticker_name}_{bar_size.value}_{current_datetime_str}.png")
        