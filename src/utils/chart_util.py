import datetime
import math
import threading
import pandas as pd
import mplfinance as mpf
from matplotlib import dates, ticker
import matplotlib as mpl
mpl.use('Agg')

from constant.indicator.scatter_colour import ScatterColour
from constant.indicator.scatter_symbol import ScatterSymbol
from constant.indicator.customised_indicator import CustomisedIndicator

from utils.config_util import get_config
from utils.dataframe_util import get_candle_comments_df, get_scatter_symbol_and_colour_df
from utils.datetime_util import get_offsetted_hit_scanner_datetime
from utils.math_util import get_max_round_decimal_places, round_to_nth_digit, get_first_non_zero_decimal_place_position
from utils.logger import Logger

from constant.candle.bar_size import BarSize
from constant.indicator.indicator import Indicator

idx = pd.IndexSlice
logger = Logger()

#candle_stick_chart_generation_lock = threading.Lock()

CHART_ROOT_DIR = get_config('CHART_SETTING', 'PATH')

CHART_WIDTH_PIXEL = get_config('CHART_SETTING', 'CHART_WIDTH_PIXEL')
CHART_HEIGHT_PIXEL = get_config('CHART_SETTING', 'CHART_HEIGHT_PIXEL')

PRICE_GRID_DIVISION = get_config('CHART_SETTING', 'PRICE_GRID_DIVISION')
VOLUME_GRID_DIVISION = get_config('CHART_SETTING', 'VOLUME_GRID_DIVISION')

MINUTE_CANDLE_DISPLAY_FORMAT = '%H:%M'
DAILY_CANDLE_DISPLAY_FORMAT = '%Y-%m-%d'

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
                            width_adjuster_version='v0',
                            figsize =(CHART_WIDTH_PIXEL/100, CHART_HEIGHT_PIXEL/100),
                            style=DEFAULT_CHART_STYLE,
                            scale_width_adjustment=dict(volume=1.5,candle=1.5),
                            tight_layout=True,
                            panel_ratios=(1,0.3),
                            figratio=(16,9),
                            figscale=0.8)
                            
DESCRIPTION_X_AXIS_OFFSET = -0.35
INDICATOR_Y_AXIS_TICK_OFFSET_FACTOR = 2
MIN_PRICE_RANGE_Y_AXIS_TICK_OFFSET_FACTOR = 2

def generate_chart(pattern: str, 
                   bar_size: BarSize, 
                   main_df: pd.DataFrame, 
                   hit_scanner_datetime: pd.Timestamp,
                   daily_date_to_fake_minute_datetime_x_axis_dict:dict, 
                   scatter_symbol_df = None, 
                   scatter_colour_df = None, 
                   description_df = None, 
                   description_offset: float = DESCRIPTION_X_AXIS_OFFSET) -> str:
    simple_chart = True if len(main_df) > 7 else False
    chart_setting = COMMON_CHART_SETTING
    ticker_name = main_df.columns.get_level_values(0).values[0]
    
    scatter_size = 600
    description_font_size = 14

    ticker_level_dropped_df = main_df.copy()
    ticker_level_dropped_df.columns = ticker_level_dropped_df.columns.droplevel(0)

    price_min_range = ticker_level_dropped_df[Indicator.LOW.value].min()
    price_max_range = ticker_level_dropped_df[Indicator.HIGH.value].max()
    volume_max_range = ticker_level_dropped_df[Indicator.VOLUME.value].max()

    price_range_difference = price_max_range - price_min_range
    
    if price_range_difference <= 0:
        return None
    
    price_y_axis_grid_tick = (price_range_difference / PRICE_GRID_DIVISION)
    round_precision = get_max_round_decimal_places(price_min_range)
    first_non_zero_decimal_place = get_first_non_zero_decimal_place_position(price_y_axis_grid_tick)
    round_precision = max(first_non_zero_decimal_place, round_precision)
    price_y_axis_grid_tick = round(price_y_axis_grid_tick, round_precision)
    
    if price_y_axis_grid_tick <= 0:
        return None
    
    no_of_price_tick =  math.ceil(price_range_difference / price_y_axis_grid_tick)
        
    low_offset_idx = 0 
    high_offset_idx = no_of_price_tick + 2
    scatter_symbol_multiplier = 0.88
    if price_y_axis_grid_tick < 1:
        low_offset_idx = -2
        high_offset_idx += 1
        scatter_symbol_multiplier = 0.95
    
    full_price_tick_list = [price_min_range + (i * price_y_axis_grid_tick) for i in range(low_offset_idx, high_offset_idx)]

    round_volume_max_range = round_to_nth_digit(volume_max_range, 1)
    volume_y_axis_grid_tick = round_to_nth_digit((round_volume_max_range / VOLUME_GRID_DIVISION), 2)
    volume_tick_list = [0 + (i * volume_y_axis_grid_tick) for i in range(VOLUME_GRID_DIVISION + 1)]

    dt_str_list = []
    x_axis_unit = None
    description_dict_list = []

    if bar_size == BarSize.ONE_DAY:
        #dt_str_list = [dt.strftime(DAILY_CANDLE_DISPLAY_FORMAT) for dt in main_df.index.tolist()]
        #x_axis_unit = dates.AutoDateLocator(minticks=1, interval_multiples=False)
        chart_setting.update(dict(datetime_format = DAILY_CANDLE_DISPLAY_FORMAT, 
                                  xrotation=0,
                                  show_nontrading=False,
                                  scale_padding=dict(left=0.5, right=3, bottom=1.5, top=2)))
    elif bar_size == BarSize.ONE_MINUTE:
        dt_str_list = []
        for dt in main_df.index.tolist():
            if dt not in daily_date_to_fake_minute_datetime_x_axis_dict:
                dt_str = dt.strftime((MINUTE_CANDLE_DISPLAY_FORMAT)) 
            else:
                dt_str = daily_date_to_fake_minute_datetime_x_axis_dict.get(dt).strftime((DAILY_CANDLE_DISPLAY_FORMAT))
            
            dt_str_list.append(dt_str)

        x_axis_unit = dates.MinuteLocator(interval=1)
        chart_setting.update(dict(datetime_format = MINUTE_CANDLE_DISPLAY_FORMAT, 
                                  xrotation=0,
                                  show_nontrading=True,
                                  scale_padding=dict(left=0.5, right=3, bottom=1, top=2)))

    if description_df is not None:
        row_counter = 0
        for idx_datetime, description_series in description_df.iterrows():
            if bar_size == BarSize.ONE_DAY:
                #https://stackoverflow.com/questions/70341767/how-to-add-a-string-comment-above-every-single-candle-using-mplfinance-plot-or
                #x_axis_val = dates.date2num((idx_datetime + pd.Timedelta(days=description_offset)).to_pydatetime())
                x_axis_val = row_counter
                row_counter += 1
            elif bar_size == BarSize.ONE_MINUTE:
                x_axis_val = dates.date2num((idx_datetime + pd.Timedelta(minutes=description_offset)).to_pydatetime())

            description_dict_list.append(
                dict(x_axis = x_axis_val, 
                     y_axis = main_df.loc[idx_datetime, (ticker_name, Indicator.HIGH.value)],
                     description = description_series[0]
            ))
            
    if simple_chart and bar_size == BarSize.ONE_MINUTE:
        last_key = list(daily_date_to_fake_minute_datetime_x_axis_dict)[-1]
        last_daily_date = daily_date_to_fake_minute_datetime_x_axis_dict.get(last_key).strftime((DAILY_CANDLE_DISPLAY_FORMAT))
        last_daily_date_idx = dt_str_list.index(last_daily_date)
        hit_scanner_date_idx = dt_str_list.index(hit_scanner_datetime.strftime((MINUTE_CANDLE_DISPLAY_FORMAT)))
        
        simpified_dt_str_list = []
        for i, dt in enumerate(dt_str_list):
            if (i == last_daily_date_idx 
                    or i == len(dt_str_list) - 1 
                    or i == hit_scanner_date_idx):
                simpified_dt_str_list.append(dt)
            else:
                simpified_dt_str_list.append('')
        
        dt_str_list = simpified_dt_str_list
        
        for i, description_dict in enumerate(description_dict_list):
            if not dt_str_list[i]:
                description_dict['description'] = ''
        
        scatter_size = 200
        description_font_size = 12

    if scatter_symbol_df is not None and scatter_colour_df is not None:
        indicator_plot = mpf.make_addplot((ticker_level_dropped_df[Indicator.LOW.value] * scatter_symbol_multiplier),
                                          type='scatter', 
                                          markersize=scatter_size, 
                                          marker=scatter_symbol_df.values.flatten(), 
                                          color=scatter_colour_df.values.flatten())

        indicator_chart_setting = dict(addplot=indicator_plot)
        chart_setting.update(indicator_chart_setting)

    chart, axis_list = mpf.plot(ticker_level_dropped_df,
                                returnfig=True,
                                **chart_setting)

    if bar_size == BarSize.ONE_DAY:
        chart.suptitle(f'{ticker_name} - 1D', fontsize=35, color='white')
    elif bar_size == BarSize.ONE_MINUTE:
        chart.suptitle(f'{ticker_name} - 1m', fontsize=35, color='white')

    if description_dict_list:
        for description in description_dict_list:
            axis_list[0].text(description['x_axis'], 
                              description['y_axis'], 
                              description['description'], 
                              fontsize=description_font_size, 
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
        axis_list[2].set_yticklabels([f'{"{:,}".format(int(tick))}' for tick in volume_tick_list])
        axis_list[2].yaxis.set_major_locator(ticker.MultipleLocator(volume_y_axis_grid_tick))

    if dt_str_list:
        new_x_ticks = [dates.date2num(dt) for dt in main_df.index.tolist()]
        axis_list[3].set_xticks(new_x_ticks)
        axis_list[3].set_xticklabels(dt_str_list)
        
        if x_axis_unit:
            axis_list[3].xaxis.set_major_locator(x_axis_unit)

    current_datetime_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    output_dir =  f"{CHART_ROOT_DIR}/{pattern}_{ticker_name}_{bar_size.value}_{current_datetime_str}.png"
    chart.savefig(output_dir)
    del chart
    
    logger.log_debug_msg(f'Chart {output_dir} has been generated', with_std_out=True)
    
    return output_dir
    
def get_candlestick_chart(candle_data_df: pd.DataFrame, 
                          ticker: str, pattern: str, bar_size: BarSize,
                          hit_scanner_datetime: pd.Timestamp, 
                          scatter_symbol: ScatterSymbol, scatter_colour: ScatterColour,
                          daily_date_to_fake_minute_datetime_x_axis_dict: dict = None,
                          positive_offset: int = None, negative_offset: int = None,
                          candle_comment_list: list = [CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.GAP_PCT_CHANGE, Indicator.CLOSE, Indicator.VOLUME]) -> str:
    #with candle_stick_chart_generation_lock:
        symbol_df, colour_df = get_scatter_symbol_and_colour_df(src_df=candle_data_df.loc[:, idx[[ticker], Indicator.LOW.value]], 
                                                                occurrence_idx_list=[hit_scanner_datetime], 
                                                                scatter_symbol=scatter_symbol, 
                                                                scatter_colour=scatter_colour)
        description_df = get_candle_comments_df(candle_data_df.loc[:, idx[[ticker], :]], 
                                                indicator_list=candle_comment_list)
        candle_start_range, candle_end_range = get_offsetted_hit_scanner_datetime(hit_scanner_datetime=hit_scanner_datetime, 
                                                                                  indice=candle_data_df.index,
                                                                                  negative_offset=negative_offset, 
                                                                                  positive_offset=positive_offset)

        logger.log_debug_msg(f'{ticker} candle start range: {candle_start_range}, candle end range: {candle_end_range}')

        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 3,):
            logger.log_debug_msg(f'{ticker} candle chart data:')
            logger.log_debug_msg(candle_data_df.loc[candle_start_range:candle_end_range, idx[[ticker], :]])

        chart_dir = generate_chart(pattern=pattern, 
                                   bar_size=bar_size,
                                   hit_scanner_datetime=hit_scanner_datetime,
                                   daily_date_to_fake_minute_datetime_x_axis_dict=daily_date_to_fake_minute_datetime_x_axis_dict,
                                   main_df=candle_data_df.loc[candle_start_range:candle_end_range, idx[[ticker], :]],
                                   scatter_symbol_df=symbol_df.loc[candle_start_range:candle_end_range, :],
                                   scatter_colour_df=colour_df.loc[candle_start_range:candle_end_range, :],
                                   description_df=description_df.loc[candle_start_range:candle_end_range, :])

        return chart_dir
   